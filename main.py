import config
import json
import os
from modules import (
    IngestionPipeline,
    DBManager,
    SQLGenerator,
    HybridEvaluator
)
from transformers import (
    AutoTokenizer, 
    AutoModelForCausalLM, 
    AutoModelForSeq2SeqLM, 
    BitsAndBytesConfig
)
import torch
from ast_gen.ast_parser import SQLASTParser

def initialize_model():
    print(f"--- Initializing Model: {config.MODEL_ID} ---")
    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_ID)
    
    if getattr(config, "MODEL_TYPE", "causal") == "seq2seq":
        model = AutoModelForSeq2SeqLM.from_pretrained(
            config.MODEL_ID,
            device_map="auto"
        )
    else:
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16
        )
        tokenizer.pad_token = tokenizer.eos_token
        model = AutoModelForCausalLM.from_pretrained(
            config.MODEL_ID,
            quantization_config=bnb_config,
            device_map="auto"
        )

    model.eval()
    return model, tokenizer

def main():
    print("========================================")
    print("      AssessSQL Pipeline Initiated      ")
    print("========================================")

    model, tokenizer = initialize_model()

    print("\n--- Level 1: Running Ingestion Module ---")
    ingestion = IngestionPipeline(pdf_path=config.PDF_PATH, output_dir=config.DATA_DIR)
    ingestion.run()

    if not ingestion.dataset:
        print("[!] Error: No data loaded from PDF. Exiting.")
        return

    print("\n--- Level 1: Generating Ground Truth Library with ASTs ---")
    generator = SQLGenerator(model, tokenizer)
    ast_parser = SQLASTParser() #
    gt_library = {} 

    for item in ingestion.dataset:
        db_id = item['db_id']
        schema = ingestion.schema_cache.get(db_id)
        
        raw_gt_queries = generator.generate_queries(schema, item['question'], num_sequences=5)
        
        gt_with_asts = []
        for sql in raw_gt_queries:
            try:
                ast_root = ast_parser.parse_sql_to_ast(sql)
                ast_dict = ast_parser.ast_to_dict(ast_root)
                gt_with_asts.append({
                    "sql": sql,
                    "ast": ast_dict
                })
            except Exception as e:
                print(f"[!] AST generation failed for: {sql}. Error: {e}")

        gt_library[item['question']] = gt_with_asts

    lib_path = os.path.join(config.DATA_DIR, "gt_library_with_asts.json")
    with open(lib_path, "w") as f:
        json.dump(gt_library, f, indent=2)
    print(f"Success: Level 1 Library saved to {lib_path}")

    print("\n--- Level 2: Starting Evaluation Phase ---")
    
    with open(lib_path, "r") as f:
        loaded_library = json.load(f)

    db_manager = DBManager(config.SCHEMA_DIR)
    evaluator = HybridEvaluator(db_manager)
    
    sample_question = list(loaded_library.keys())[0]
    sample_gt_data = loaded_library[sample_question]
    
    student_submission = "SELECT * FROM EMP"
    rubric = {"projections": 20, "tables": 30, "filters": 50}

    print(f"\n[EVALUATING SUBMISSION FOR]: {sample_question}")
    report = evaluator.evaluate(student_submission, sample_gt_data, rubric)
    # generator.run_pipeline(
    #     dataset=ingestion.dataset,
    #     schema_cache=ingestion.schema_cache,
    #     schema_dir=config.SCHEMA_DIR
    # )
    
    print("\n========================================")
    print("      Pipeline Finished Successfully    ")
    print("========================================")
    print(f"Logical Similarity: {report['similarity_score'] * 100}%")
    print(f"Final Rubric Grade: {report['rubric_grade']} / 100")
    print(f"Execution Match:    {report['execution_match']}")
    print("\n[PEDAGOGICAL FEEDBACK]:")
    for msg in report['pedagogical_feedback']:
        print(f" - {msg}")

if __name__ == "__main__":
    main()