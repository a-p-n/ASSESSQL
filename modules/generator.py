import torch
import sqlite3
import os
import json
import config
from tqdm import tqdm

class SQLGenerator:
    def __init__(self, model, tokenizer):
        self.model = model
        self.tokenizer = tokenizer
        self.device = model.device
        self.is_seq2seq = getattr(config, "MODEL_TYPE", "causal") == "seq2seq"

    def _run_query(self, db_path, query):
        """Executes SQL against the SQLite file."""
        if any(k in query.upper() for k in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]):
            return "error", "Destructive queries are not allowed."
        try:
            # Connect in read-only mode
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            conn.close()
            return "success", result
        except Exception as e:
            return "error", str(e)

    def _build_prompt(self, schema, question, error_data=None):
        """
        Constructs the prompt. T5 generally prefers simpler prompts than Causal models.
        """
        # Format for T5/Seq2Seq
        if self.is_seq2seq:
            if error_data:
                # T5 correction prompt
                return f"fix sql error: {error_data['error_message']} query: {error_data['failed_sql']} question: {question} schema: {schema}"
            # Standard T5 prompt
            return f"translate to SQL: {question} schema: {schema}"
        
        # Format for SQLCoder/Causal Models
        if error_data:
            return f"### Task\nCorrect the following SQL query based on the error.\nQuestion: {question}\nSchema: {schema}\nFailed Query: {error_data['failed_sql']}\nError: {error_data['error_message']}\n### Corrected SQL\n```sql\n"
        
        return f"### Task\nGenerate a SQL query to answer: {question}\n### Database Schema\n{schema}\n### SQL\n"

    def generate_queries(self, schema, question, num_sequences=5, error_data=None):
        prompt = self._build_prompt(schema, question, error_data)
        
        # Adjust beams for correction vs exploration
        num_beams = 3 if error_data else (num_sequences + 2)
        num_return_sequences = 1 if error_data else num_sequences

        inputs = self.tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(self.device)

        with torch.no_grad():
            output_sequences = self.model.generate(
                input_ids=inputs["input_ids"],
                attention_mask=inputs["attention_mask"],
                max_new_tokens=512,
                num_beams=num_beams,
                num_return_sequences=num_return_sequences,
                early_stopping=True
            )

        generated_queries = []
        for sequence in output_sequences:
            decoded = self.tokenizer.decode(sequence, skip_special_tokens=True)
            
            # PARSING LOGIC
            # Case A: Causal Model output (often Markdown)
            if "```sql" in decoded:
                sql = decoded.split("```sql")[1].split("```")[0].strip()
            # Case B: T5 output (Raw text)
            elif "SELECT" in decoded.upper():
                # If T5 output has extra text, try to grab from SELECT onwards
                if "SELECT" in decoded:
                     sql = "SELECT" + decoded.split("SELECT", 1)[1]
                else:
                     sql = decoded.strip() # Trust the model output
            else:
                sql = decoded.strip() # Fallback
            
            generated_queries.append(sql)

        return generated_queries

    def run_pipeline(self, dataset, schema_cache, schema_dir):
        print(f"\n--- Generator Started: Processing {len(dataset)} questions ---")
        evaluation_results = []
        
        for item in tqdm(dataset, desc="Generating"):
            try:
                question = item['question']
                db_id = item['db_id']
                # Note: In a real scenario, you won't have 'gold_query' from the PDF usually.
                # If you do manual labeling later, you can add it back. 
                # For now, we assume we might NOT have a gold query to compare against.
                gold_query = item.get('query', '') 

                schema = schema_cache.get(db_id)
                if not schema: continue

                db_path = os.path.join(schema_dir, db_id, f"{db_id}.sqlite") # Note: This DB file needs to actually exist!
                
                # 1. Generate Prediction
                predicted_sqls = self.generate_queries(schema, question, num_sequences=3)
                best_prediction = predicted_sqls[0] if predicted_sqls else "Failed"
                
                # 2. Syntax Check (Run against DB to see if it crashes)
                # Since we likely don't have the DB populated with data yet, this just checks syntax validity.
                valid_syntax = False
                syntax_error = None
                
                if os.path.exists(db_path):
                     status, result = self._run_query(db_path, best_prediction)
                     if status == "success":
                         valid_syntax = True
                     else:
                         syntax_error = result
                else:
                    # If DB doesn't exist yet, we skip execution check
                    status = "skipped_no_db"

                # 3. Self-Correction (If syntax error found)
                if status == "error":
                    error_data = {
                        "question": question, 
                        "failed_sql": best_prediction, 
                        "error_message": syntax_error
                    }
                    corrected = self.generate_queries(schema, question, error_data=error_data)
                    if corrected:
                        best_prediction = corrected[0] + " (Corrected)"

                evaluation_results.append({
                    "question": question,
                    "db_id": db_id,
                    "generated_sql": best_prediction,
                    "syntax_valid": valid_syntax,
                    "error_log": syntax_error
                })

            except Exception as e:
                print(f"Error processing question: {e}")
            
            torch.cuda.empty_cache()

        # Save Results
        output_file = os.path.join(config.DATA_DIR, "final_results.json")
        with open(output_file, "w") as f:
            json.dump(evaluation_results, f, indent=2)
        
        print(f"Results saved to {output_file}")