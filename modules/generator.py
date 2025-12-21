import torch
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

class GroundTruthGenerator:
    def __init__(self, model_name, device):
        self.device = device
        print(f"[INFO] Loading Seq2Seq Model: {model_name} on {device}...")
        
        # CHANGED: Use Seq2SeqLM for T5
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_name).to(device)

    def generate_gold_query(self, question, schema_context):
        """
        Generates the 'Gold' SQL query using T5.
        """
        # T5 typically expects a prefix like "translate English to SQL:" 
        # combined with the input.
        input_text = f"translate English to SQL: {question} Schema: {schema_context}"
        
        inputs = self.tokenizer(
            input_text, 
            return_tensors="pt", 
            max_length=512, 
            truncation=True
        ).to(self.device)
        
        # Generate output
        outputs = self.model.generate(
            **inputs, 
            max_length=150,
            num_beams=4,        # T5 works better with beam search
            early_stopping=True
        )
        
        generated_sql = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        return generated_sql.strip()