from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import json

from ast_gen.models import ASTNode, ASTNodeType, ASTComparisonResult

class ComparisonType(str, Enum):
    EXACT_MATCH = "exact_match"
    STRUCTURAL_SIMILARITY = "structural_similarity"

@dataclass
class NodeMatch:
    student_node: Dict[str, Any]
    ground_truth_node: Dict[str, Any]
    similarity_score: float
    match_type: str
    differences: List[str]

class ASTComparator:
    def __init__(self):
        self.node_weights = {
            'SELECT': 3.0,
            'FROM': 2.0,
            'WHERE': 2.5,
            'JOIN': 4.0,
            'GROUP_BY': 3.5,
            'HAVING': 3.0,
            'ORDER_BY': 2.0,
            'LIMIT': 1.5,
            'COLUMN': 1.0,
            'TABLE': 2.0,
            'CONDITION': 2.0,
            'FUNCTION': 2.5,
            'OPERATOR': 1.5,
            'LITERAL': 1.0
        }
    
    def compare_asts(self, student_ast: Dict[str, Any], 
                    ground_truth_asts: List[Dict[str, Any]],
                    comparison_type: ComparisonType = ComparisonType.STRUCTURAL_SIMILARITY) -> ASTComparisonResult:
        best_match = None
        best_score = 0.0
        
        for i, gt_ast in enumerate(ground_truth_asts):
            match_result = self._compare_single_ast(student_ast, gt_ast, comparison_type)
            match_result['ground_truth_index'] = i
            
            if match_result['similarity_score'] > best_score:
                best_score = match_result['similarity_score']
                best_match = match_result
        
        feedback = self._generate_feedback(student_ast, best_match, comparison_type)
        
        return ASTComparisonResult(
            similarity_score=best_score,
            matching_nodes=best_match['matching_nodes'] if best_match else 0,
            total_nodes=best_match['total_nodes'] if best_match else 0,
            differences=best_match['differences'] if best_match else [],
            feedback=feedback
        )

    def calculate_rubric_score(self, student_ast_dict: Dict[str, Any], 
                               best_gt_ast_dict: Dict[str, Any], 
                               rubric_weights: Dict[str, float]) -> float:
        total_score = 0.0
        
        component_to_type = {
            "projections": "SELECT",
            "tables": "FROM",
            "filters": "WHERE",
            "grouping": "GROUP_BY",
            "having": "HAVING",
            "sorting": "ORDER_BY",
            "limit": "LIMIT"
        }

        def find_clause_node(node, target_type):
            if node.get("node_type") == target_type:
                return node
            for child in node.get("children", []):
                res = find_clause_node(child, target_type)
                if res: return res
            return None

        for component, weight in rubric_weights.items():
            target_type = component_to_type.get(component.lower())
            if not target_type:
                continue
            
            gt_clause = find_clause_node(best_gt_ast_dict, target_type)
            st_clause = find_clause_node(student_ast_dict, target_type)
            
            if gt_clause and st_clause:
                local_similarity = self._structural_similarity_comparison(st_clause, gt_clause)
                total_score += weight * local_similarity['similarity_score']
                
        return round(total_score, 2)
    
    def _compare_single_ast(self, student_ast: Dict[str, Any], 
                           ground_truth_ast: Dict[str, Any],
                           comparison_type: ComparisonType) -> Dict[str, Any]:
        if comparison_type == ComparisonType.EXACT_MATCH:
            return self._exact_match_comparison(student_ast, ground_truth_ast)
        return self._structural_similarity_comparison(student_ast, ground_truth_ast)
    
    def _exact_match_comparison(self, student_ast: Dict[str, Any], 
                               ground_truth_ast: Dict[str, Any]) -> Dict[str, Any]:
        is_exact = self._are_asts_identical(student_ast, ground_truth_ast)
        
        return {
            'similarity_score': 1.0 if is_exact else 0.0,
            'matching_nodes': self._count_nodes(student_ast) if is_exact else 0,
            'total_nodes': self._count_nodes(ground_truth_ast),
            'differences': [] if is_exact else ["ASTs are not identical"],
            'match_type': 'exact'
        }
    
    def _structural_similarity_comparison(self, student_ast: Dict[str, Any], 
                                        ground_truth_ast: Dict[str, Any]) -> Dict[str, Any]:
        matches = []
        differences = []
        total_nodes = self._count_nodes(ground_truth_ast)
        matching_nodes_count = 0
        
        self._compare_structure_recursive(student_ast, ground_truth_ast, matches, differences)
        
        weighted_score = 0.0
        total_weight = 0.0
        
        for match in matches:
            node_type = match['node_type']
            weight = self.node_weights.get(node_type, 1.0)
            weighted_score += match['similarity'] * weight
            total_weight += weight
            if match['similarity'] > 0.5:
                matching_nodes_count += 1
        
        similarity_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        return {
            'similarity_score': similarity_score,
            'matching_nodes': matching_nodes_count,
            'total_nodes': total_nodes,
            'differences': differences,
            'match_type': 'structural',
            'detailed_matches': matches
        }
    
    def _are_asts_identical(self, ast1: Dict[str, Any], ast2: Dict[str, Any]) -> bool:
        if ast1.get('node_type') != ast2.get('node_type'):
            return False
        if ast1.get('value') != ast2.get('value'):
            return False
        
        children1 = ast1.get('children', [])
        children2 = ast2.get('children', [])
        
        if len(children1) != len(children2):
            return False
        
        for child1, child2 in zip(children1, children2):
            if not self._are_asts_identical(child1, child2):
                return False
        
        return True
    
    def _compare_structure_recursive(self, student_node: Dict[str, Any], 
                                   gt_node: Dict[str, Any],
                                   matches: List[Dict[str, Any]], 
                                   differences: List[str]):
        node_type = student_node.get('node_type')
        gt_node_type = gt_node.get('node_type')
        
        if node_type == gt_node_type:
            similarity = 0.8
            if student_node.get('value') == gt_node.get('value'):
                similarity = 1.0
            
            matches.append({
                'node_type': node_type,
                'similarity': similarity,
                'student_value': student_node.get('value'),
                'gt_value': gt_node.get('value')
            })
        else:
            differences.append(f"Node type mismatch: {node_type} vs {gt_node_type}")
        
        student_children = student_node.get('children', [])
        gt_children = gt_node.get('children', [])
        
        for i, (child, gt_child) in enumerate(zip(student_children, gt_children)):
            self._compare_structure_recursive(child, gt_child, matches, differences)
    
    def _count_nodes(self, ast: Dict[str, Any]) -> int:
        count = 1
        for child in ast.get('children', []):
            count += self._count_nodes(child)
        return count
    
    def _generate_feedback(self, student_ast: Dict[str, Any], 
                          best_match: Optional[Dict[str, Any]], 
                          comparison_type: ComparisonType) -> List[str]:
        feedback = []
        
        if not best_match:
            feedback.append("No suitable logical match found in ground truth library.")
            return feedback
        
        score = best_match['similarity_score']
        
        if score >= 0.9:
            feedback.append("Excellent! Your query structure is logically equivalent to the reference.")
        elif score >= 0.7:
            feedback.append("Good work! Your query is mostly correct with minor logical differences.")
        elif score >= 0.5:
            feedback.append("The query has the right structural foundation but needs clause improvements.")
        else:
            feedback.append("Your query needs revision to align with the required schema intent.")
        
        for diff in best_match.get('differences', []):
            feedback.append(f"Localization: {diff}")
        
        return feedback
