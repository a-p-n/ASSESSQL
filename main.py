import config
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

def initialize_model():
    print(f"--- Initializing Model: {config.MODEL_ID} ---")
    print(f"Using Device: {config.DEVICE}")
    print(f"Model Type: {getattr(config, 'MODEL_TYPE', 'causal')}")

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
    print("Model loaded successfully.")
    return model, tokenizer

def main():
    print("========================================")
    print("      AssessSQL Pipeline Initiated      ")
    print("========================================")

    model, tokenizer = initialize_model()

    print("\n--- Running Ingestion Module ---")
    ingestion = IngestionPipeline(
        pdf_path=config.PDF_PATH,
        output_dir=config.DATA_DIR
    )
    ingestion.run()

    if not ingestion.dataset:
        print("[!] Error: No data loaded from PDF. Exiting.")
        return

    print("\n--- Starting Generator Module ---")
    
    generator = SQLGenerator(model, tokenizer)
    
    generator.run_pipeline(
        dataset=ingestion.dataset,
        schema_cache=ingestion.schema_cache,
        schema_dir=config.SCHEMA_DIR
    )

    print("\n========================================")
    print("      Pipeline Finished Successfully    ")
    print("========================================")

if __name__ == "__main__":
    main()