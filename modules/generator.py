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
        if any(k in query.upper() for k in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]):
            return "error", "Destructive queries are not allowed."
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            conn.close()
            return "success", result
        except Exception as e:
            return "error", str(e)

    def _build_prompt(self, schema, question, error_data=None):
        if self.is_seq2seq:
            if error_data:
                return f"fix sql error: {error_data['error_message']} query: {error_data['failed_sql']} question: {question} schema: {schema}"
            return f"translate to SQL: {question} schema: {schema}"
        
        if error_data:
            return f"### Task\nCorrect the following SQL query based on the error.\nQuestion: {question}\nSchema: {schema}\nFailed Query: {error_data['failed_sql']}\nError: {error_data['error_message']}\n### Corrected SQL\n```sql\n"
        
        return f"### Task\nGenerate a SQL query to answer: {question}\n### Database Schema\n{schema}\n### SQL\n"

    def generate_queries(self, schema, question, num_sequences=5, error_data=None):
        prompt = self._build_prompt(schema, question, error_data)
        
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
            
            if "```sql" in decoded:
                sql = decoded.split("```sql")[1].split("```")[0].strip()
            elif "SELECT" in decoded.upper():
                if "SELECT" in decoded:
                     sql = "SELECT" + decoded.split("SELECT", 1)[1]
                else:
                     sql = decoded.strip()
            else:
                sql = decoded.strip()
            
            generated_queries.append(sql)

        return generated_queries

    def run_pipeline(self, dataset, schema_cache, schema_dir):
        print(f"\n--- Generator Started: Processing {len(dataset)} questions ---")
        evaluation_results = []
        
        for item in tqdm(dataset, desc="Generating"):
            try:
                question = item['question']
                db_id = item['db_id']
                gold_query = item.get('query', '') 

                schema = schema_cache.get(db_id)
                if not schema: continue

                db_path = os.path.join(schema_dir, db_id, f"{db_id}.sqlite")
                
                predicted_sqls = self.generate_queries(schema, question, num_sequences=3)
                best_prediction = predicted_sqls[0] if predicted_sqls else "Failed"
                
                valid_syntax = False
                syntax_error = None
                
                if os.path.exists(db_path):
                     status, result = self._run_query(db_path, best_prediction)
                     if status == "success":
                         valid_syntax = True
                     else:
                         syntax_error = result
                else:
                    status = "skipped_no_db"

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

        output_file = os.path.join(config.DATA_DIR, "final_results.json")
        with open(output_file, "w") as f:
            json.dump(evaluation_results, f, indent=2)
        
        print(f"Results saved to {output_file}")