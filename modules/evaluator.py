import sqlglot
from .db_manager import DBManager

class HybridEvaluator:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_ast(self, sql):
        try:
            return sqlglot.parse(sql)[0]
        except Exception as e:
            return None

    def evaluate(self, student_sql, ground_truth_sqls, rubric=None):
        if not rubric:
            rubric = {
                "projections": 20, 
                "tables": 30, 
                "filters": 50
            }

        ast_report = self.ast_processor.evaluate_submission(
            student_sql, 
            ground_truth_sqls, 
            rubric
        )

        execution_match = False
        execution_error = None
        
        student_res = self.db.execute_query(student_sql)
        if isinstance(student_res, str) and "error" in student_res.lower():
            execution_error = student_res
        else:
            for gt_sql in ground_truth_sqls:
                gold_res = self.db.execute_query(gt_sql)
                if student_res == gold_res:
                    execution_match = True
                    break

        return {
            "similarity_score": ast_report["similarity_score"],
            "rubric_grade": ast_report["final_grade"],
            "execution_match": execution_match,
            "execution_error": execution_error,
            "pedagogical_feedback": ast_report["feedback"],
            "structural_differences": ast_report["differences"]
        }