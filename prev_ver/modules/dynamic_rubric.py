from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json

from ast_gen.ast_parser import SQLASTParser
from ast_gen.models import QueryMetadata, ASTNode, ASTNodeType

class StudentQueryProcessor:
    def __init__(self):
        self.ast_parser = SQLASTParser()
        # Define node types that are worth 1 mark each
        self.scorable_types = {
            ASTNodeType.SELECT, ASTNodeType.FROM, ASTNodeType.WHERE,
            ASTNodeType.GROUP_BY, ASTNodeType.HAVING, ASTNodeType.ORDER_BY,
            ASTNodeType.LIMIT, ASTNodeType.JOIN, ASTNodeType.UNION,
            ASTNodeType.TABLE, ASTNodeType.COLUMN, ASTNodeType.CONDITION,
            ASTNodeType.FUNCTION, ASTNodeType.OPERATOR, ASTNodeType.LITERAL
        }

    def parse_student_query(self, sql_query: str, student_id: str, question_id: str) -> QueryMetadata:
        """
        Parses a student query into metadata without grading.
        """
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
            # Fallback for invalid queries
            return QueryMetadata(
                query_text=sql_query,
                ast_json={"error": str(e)},
                student_id=student_id,
                question_id=question_id,
                is_ground_truth=False,
                created_at=datetime.utcnow()
            )

    def evaluate_submission(self, student_query: str, 
                            ground_truth_queries: List[str], 
                            rubric: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        Compares the student query against a list of ground truth queries.
        Dynamically calculates total marks based on the best matching ground truth.
        """
        # 1. Parse Student Query
        try:
            student_ast = self.ast_parser.parse_sql_to_ast(student_query)
        except Exception as e:
            return {
                "error": f"Syntax Error in Student Query: {str(e)}",
                "final_grade": 0.0,
                "obtained_marks": 0,
                "total_marks": 0
            }

        # 2. Parse All Ground Truths
        gt_asts = []
        for gt in ground_truth_queries:
            try:
                gt_asts.append(self.ast_parser.parse_sql_to_ast(gt))
            except:
                continue # Skip invalid ground truths
        
        if not gt_asts:
            return {"error": "No valid ground truth queries provided."}

        best_result = None
        best_ratio = -1.0

        # 3. Find the Best Match
        for i, gt_ast in enumerate(gt_asts):
            obtained, total = self._calculate_tree_score(student_ast, gt_ast)
            
            # Calculate percentage for this specific GT match
            ratio = (obtained / total) if total > 0 else 0.0
            
            if ratio > best_ratio:
                best_ratio = ratio
                best_result = {
                    "matched_gt_query": ground_truth_queries[i],
                    "obtained_marks": obtained,
                    "total_marks": total,
                    "final_grade": round(ratio * 100, 2), # Score out of 100
                    "similarity_score": round(ratio, 4)
                }

        return best_result

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

    # --- Internal Scoring Logic ---

    def _calculate_tree_score(self, student_node: Optional[ASTNode], gt_node: ASTNode) -> Tuple[int, int]:
        """
        Recursively calculates (obtained_marks, total_marks) by comparing the ASTs.
        Uses a greedy strategy to match children nodes.
        """
        current_obtained = 0
        current_total = 0

        # 1. Score the Current Node
        # Check if this node type is worth marks in the rubric
        is_scorable = gt_node.node_type in self.scorable_types
        
        if is_scorable:
            current_total += 1
            # If student node exists and matches type & value
            if student_node and self._nodes_match(student_node, gt_node):
                current_obtained += 1
        
        # 2. Score the Children (Greedy Matching)
        # We must align GT children with Student children to maximize the score
        
        gt_children = gt_node.children
        st_children = student_node.children if student_node else []
        
        used_st_indices = set()
        
        for gt_child in gt_children:
            # Calculate max possible points for this branch (recurse with None to just sum totals)
            _, branch_total = self._calculate_tree_score(None, gt_child)
            current_total += branch_total
            
            # Find the best matching child in the student's AST
            best_branch_obtained = 0
            best_st_idx = -1
            found_match = False
            
            for idx, st_child in enumerate(st_children):
                if idx in used_st_indices:
                    continue
                
                # Only compare if types generally align to save recursion depth
                if st_child.node_type == gt_child.node_type:
                    obt, _ = self._calculate_tree_score(st_child, gt_child)
                    
                    # We pick the match that yields the highest score
                    if obt > best_branch_obtained:
                        best_branch_obtained = obt
                        best_st_idx = idx
                        found_match = True
                    elif obt == best_branch_obtained and obt > 0 and not found_match:
                         # Tie-breaker: take the first valid match
                        best_st_idx = idx
                        found_match = True
            
            # If we found a matching child, add its score and mark it as used
            if found_match and best_st_idx != -1:
                current_obtained += best_branch_obtained
                used_st_indices.add(best_st_idx)

        return current_obtained, current_total

    def _nodes_match(self, node1: ASTNode, node2: ASTNode) -> bool:
        """
        Determines if two nodes are a 'match' (Correct Operation/Relation/Attribute).
        """
        if node1.node_type != node2.node_type:
            return False
        
        # Normalize values for comparison (case-insensitive)
        val1 = str(node1.value).strip().upper() if node1.value else ""
        val2 = str(node2.value).strip().upper() if node2.value else ""
        
        # For String Literals, we might want exact case, but for SQL keywords/columns, usually insensitive.
        # Assuming insensitive for now as per standard SQL grading.
        return val1 == val2