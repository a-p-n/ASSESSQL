import sys
from typing import List, Dict, Any

try:
    from modules.pdf_extractor import PDFExtractor
    from modules.grader import Grader
except ImportError:
    print("Error: Could not import modules. Make sure you are in the ASSESSQL directory.")
    sys.exit(1)

class AssessQLPipeline:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.grouped_data = {}
        self.master_schema = {}
        self.ground_truths = {}
        self.grader = None

    def run(self):
        print(f"--- [PHASE 1] INITIALIZING PIPELINE FOR '{self.pdf_path}' ---")
        
        self._extract_context()
        self._build_master_schema()
        
        self.grader = Grader(self.master_schema)
        
        self._generate_mock_ground_truths()
        
        self._run_student_simulation()

    def _extract_context(self):
        print("\n> Extracting Schema and Questions from PDF...")
        extractor = PDFExtractor(self.pdf_path)
        self.grouped_data = extractor.process()
        
        groups_found = [k for k in self.grouped_data.keys() if self.grouped_data[k]['queries'] or self.grouped_data[k]['tables']]
        print(f"  Success! Found Content in Groups: {groups_found}")

    def _build_master_schema(self):
        for group, content in self.grouped_data.items():
            tables = content.get('tables', {})
            for t_name, t_cols in tables.items():
                if t_name not in self.master_schema:
                    self.master_schema[t_name] = {}
                
                for c_name, c_data in t_cols.items():
                    col_type = c_data.get('type', 'varchar') if isinstance(c_data, dict) else c_data
                    self.master_schema[t_name][c_name] = col_type

    def _generate_mock_ground_truths(self):
        print("\n--- [PHASE 2] GROUND TRUTH ENTRY ---")
        
        for q_group, content in self.grouped_data.items():
            queries = content.get('queries', [])
            if not queries:
                continue
                
            print(f"\n=====================================")
            print(f" [{q_group}] ")
            print(f"=====================================")
            if content.get('instruction'):
                print(f"Context: {content['instruction']}\n")
                
            for i, query in enumerate(queries):
                print(f"-> Query {i+1}: {query}")
                
                gt_list = []
                x = input("   Input a ground truth (or press enter to skip): ").strip()
                while x:
                    gt_list.append(x)
                    x = input("   Input another ground truth (or press enter to move to next query): ").strip()
                
                if gt_list:
                    self.ground_truths[(q_group, i)] = gt_list

    def _run_student_simulation(self):
        print("\n--- [PHASE 3] STUDENT EVALUATION ---")
        
        if not self.ground_truths:
            print("No ground truths were entered. Cannot evaluate.")
            return
            
        target_key = list(self.ground_truths.keys())[0]
        q_group, q_idx = target_key
        
        question_text = self.grouped_data[q_group]['queries'][q_idx]
        gt_sqls = self.ground_truths[target_key]
        
        print(f"\nEvaluating for [{q_group}] -> Query {q_idx + 1}")
        print(f"Task: {question_text}")
        print("-" * 60)
        
        student_attempts = []
        x = input("Input a student query (or press enter to exit): ").strip()
        while x:
            student_attempts.append(x)
            x = input("Input another student query (or press enter to exit): ").strip()
        
        for sql in student_attempts:
            print(f"\nInput: {sql}")
            
            result = self.grader.evaluate(sql, gt_sqls)
            
            if "error" in result:
                print(f"  STATUS: REJECTED ({result['error']})")
                print("  SCORE:  0.0%")
            else:
                print(f"  STATUS: GRADED")
                print(f"  SCORE:  {result['percentage']}% ({result['obtained_marks']}/{result['total_marks']})")
                if result['percentage'] < 100:
                    print(f"  FEEDBACK: Missing features {result.get('feedback', {}).get('missing', [])}")
                    print(f"            Extra features   {result.get('feedback', {}).get('extras', [])}")

if __name__ == "__main__":
    pipeline = AssessQLPipeline("lab1.pdf")
    pipeline.run()