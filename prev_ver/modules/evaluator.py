from .db_manager import DBManager
from ast_gen.student_module import StudentQueryProcessor

class HybridEvaluator:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        self.ast_processor = StudentQueryProcessor()

    def evaluate(self, student_sql, ground_truth_package, rubric=None):
        if not rubric:
            rubric = {"projections": 20, "tables": 30, "filters": 50}

        db_id = ground_truth_package["db_id"]
        gt_variants = ground_truth_package["variants"]
        gt_sqls = [v['sql'] for v in gt_variants]

        ast_report = self.ast_processor.evaluate_submission(
            student_sql, 
            gt_sqls, 
            rubric
        )

        execution_match = False
        execution_error = None
        
        student_res = self.db.execute_query(student_sql, db_id)
        if isinstance(student_res, str) and "Error" in student_res:
            execution_error = student_res
        else:
            for gt_sql in gt_sqls:
                gold_res = self.db.execute_query(gt_sql, db_id)
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