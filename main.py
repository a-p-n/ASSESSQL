import config  # Imports settings from config.py
from modules import (
    IngestionPipeline,  # Matches your __init__.py
    DBManager,
    SQLGenerator,       # Matches your __init__.py
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
    """
    Initializes the model based on config.py settings.
    Handles the difference between T5 (Seq2Seq) and SQLCoder (Causal).
    """
    print(f"--- Initializing Model: {config.MODEL_ID} ---")
    print(f"Using Device: {config.DEVICE}")
    print(f"Model Type: {getattr(config, 'MODEL_TYPE', 'causal')}")

    tokenizer = AutoTokenizer.from_pretrained(config.MODEL_ID)
    
    # Check if we are using a Seq2Seq model (like T5) or Causal (like Llama/SQLCoder)
    if getattr(config, "MODEL_TYPE", "causal") == "seq2seq":
        # T5-Base is small enough to run without 4-bit quantization usually
        model = AutoModelForSeq2SeqLM.from_pretrained(
            config.MODEL_ID,
            device_map="auto"
        )
    else:
        # Configuration for larger Causal models (SQLCoder, etc.)
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

    # 1. LOAD MODEL
    # We do this first so we fail fast if the model is wrong
    model, tokenizer = initialize_model()

    # 2. RUN INGESTION
    # Parses the PDF and saves the JSON/Schema files to disk
    print("\n--- Running Ingestion Module ---")
    ingestion = IngestionPipeline(
        pdf_path=config.PDF_PATH,
        output_dir=config.DATA_DIR
    )
    ingestion.run()

    if not ingestion.dataset:
        print("[!] Error: No data loaded from PDF. Exiting.")
        return

    # 3. START GENERATOR
    print("\n--- Starting Generator Module ---")
    
    # Initialize generator with the loaded model
    generator = SQLGenerator(model, tokenizer)
    
    # Run evaluation using the data extracted by ingestion
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