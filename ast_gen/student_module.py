from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from ast_parser import SQLASTParser
from ast_comparison import ASTComparator, ComparisonType
from models import QueryMetadata, StudentSubmission, ASTNode, ASTComparisonResult

class StudentQueryProcessor:
    def __init__(self):
        self.ast_parser = SQLASTParser()
        self.comparator = ASTComparator()
        
    def parse_student_query(self, sql_query: str, student_id: str, question_id: str) -> QueryMetadata:
        try:
            ast_root = self.ast_parser.parse_sql_to_ast(sql_query)
            ast_json = self.ast_parser.ast_to_dict(ast_root)
            
            return QueryMetadata(
                query_text=sql_query,
                ast_json=ast_json,
                student_id=student_id,
                question_id=question_id,
                is_ground_truth=False,
                created_at=datetime.utcnow()
            )
        except Exception as e:
            raise ValueError(f"Failed to parse student query: {str(e)}")

    def evaluate_submission(self, student_query: str, 
                            ground_truth_queries: List[str], 
                            rubric: Dict[str, float]) -> Dict[str, Any]:
        student_ast_root = self.ast_parser.parse_sql_to_ast(student_query)
        student_ast_dict = self.ast_parser.ast_to_dict(student_ast_root)
        
        gt_ast_dicts = []
        for gt_sql in ground_truth_queries:
            gt_node = self.ast_parser.parse_sql_to_ast(gt_sql)
            gt_ast_dicts.append(self.ast_parser.ast_to_dict(gt_node))
            
        comparison_result = self.comparator.compare_asts(
            student_ast_dict, 
            gt_ast_dicts, 
            comparison_type=ComparisonType.STRUCTURAL_SIMILARITY
        )
        
        best_gt_index = getattr(comparison_result, 'ground_truth_index', 0)
        best_gt_ast = gt_ast_dicts[best_gt_index]
        
        final_grade = self.comparator.calculate_rubric_score(
            student_ast_dict, 
            best_gt_ast, 
            rubric
        )
        
        return {
            "similarity_score": round(comparison_result.similarity_score, 2),
            "final_grade": final_grade,
            "feedback": comparison_result.feedback,
            "differences": comparison_result.differences
        }

    def validate_query_syntax(self, sql_query: str) -> Dict[str, Any]:
        try:
            ast_root = self.ast_parser.parse_sql_to_ast(sql_query)
            has_select = any(child.node_type == 'SELECT' for child in ast_root.children)
            has_from = any(child.node_type == 'FROM' for child in ast_root.children)
            
            return {
                "is_valid": has_select and has_from,
                "missing_clauses": [c for c, present in {"SELECT": has_select, "FROM": has_from}.items() if not present]
            }
        except Exception as e:
            return {"is_valid": False, "error": str(e)}
