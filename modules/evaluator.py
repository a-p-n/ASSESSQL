import sqlglot
from .db_manager import DBManager

class HybridEvaluator:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_ast(self, sql):
        """Parses SQL into an AST (Abstract Syntax Tree)."""
        try:
            # We take the first statement found in the SQL
            return sqlglot.parse(sql)[0]
        except Exception as e:
            # If parsing fails (invalid SQL), return None
            return None

    def evaluate(self, student_sql, gold_sql, gold_ast=None):
        """
        Runs the hybrid evaluation:
        1. Structural Check (AST)
        2. Semantic Check (Execution)
        """
        report = {
            "structural_match": False, 
            "execution_match": False, 
            "score": 0,
            "execution_error": None
        }

        # --- 1. Structural Comparison (AST) ---
        student_ast = self.get_ast(student_sql)
        gold_ast = self.get_ast(gold_sql)
        
        if student_ast and gold_ast:
            # For now, we compare the string representation of the AST
            # (In the future, we can add Tree Edit Distance here)
            report['structural_match'] = (student_ast.sql() == gold_ast.sql())

        # --- 2. Semantic Comparison (Execution) ---
        # Execute Student Query
        student_res = self.db.execute_query(student_sql)
        
        # Execute Gold Query (Ground Truth)
        gold_res = self.db.execute_query(gold_sql)

        # Check for execution errors in student code
        if isinstance(student_res, str): # Our DBManager returns error strings
             report['execution_error'] = student_res
        
        # Compare results if both ran successfully
        elif not isinstance(gold_res, str):
            if student_res == gold_res:
                report['execution_match'] = True

        # --- 3. Scoring Logic ---
        # If the output data is identical, they get full points (100)
        if report['execution_match']:
            report['score'] = 100
        # If output is wrong but logic (AST) looks right, partial credit (50)
        elif report['structural_match']:
            report['score'] = 50 
        
        return report