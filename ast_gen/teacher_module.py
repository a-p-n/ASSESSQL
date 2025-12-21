"""
Teacher Level Module - Ground Truth AST Generation

This module handles the generation of ground truth SQL queries and their ASTs
based on teacher questions and evaluation rubrics.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from .ast_parser import SQLASTParser
from .database import DatabaseManager
from .models import QueryMetadata, TeacherQuestion, ASTNode

class GroundTruthGenerator:
    """
    Generates ground truth SQL queries and their ASTs from teacher questions.
    """
    
    def __init__(self):
        self.ast_parser = SQLASTParser()
        self.db_manager = DatabaseManager()
        
    def generate_ground_truth_ast(self, question: TeacherQuestion, expected_sql: str = None) -> QueryMetadata:
        """
        Generate ground truth AST from teacher question.
        
        Args:
            question: Teacher question with rubric criteria
            expected_sql: Optional pre-written SQL query
            
        Returns:
            QueryMetadata: Ground truth metadata with AST
        """
        try:
            # If no expected SQL provided, generate from question
            if not expected_sql:
                expected_sql = self._generate_sql_from_question(question)
            
            # Parse SQL to AST
            ast_root = self.ast_parser.parse_sql_to_ast(expected_sql)
            ast_json = self.ast_parser.ast_to_dict(ast_root)
            
            # Create metadata
            metadata = QueryMetadata(
                query_text=expected_sql,
                ast_json=ast_json,
                question_id=question.question_id,
                rubric_id=f"rubric_{question.question_id}",
                is_ground_truth=True,
                created_at=datetime.utcnow()
            )
            
            return metadata
            
        except Exception as e:
            raise ValueError(f"Failed to generate ground truth AST: {str(e)}")
    
    def store_ground_truth(self, metadata: QueryMetadata) -> int:
        """Store ground truth AST in database."""
        return self.db_manager.store_ground_truth_ast(metadata)
    
    def generate_multiple_ground_truths(self, question: TeacherQuestion, 
                                      alternative_queries: List[str] = None) -> List[QueryMetadata]:
        """
        Generate multiple ground truth ASTs for a question.
        
        Args:
            question: Teacher question
            alternative_queries: List of alternative correct SQL queries
            
        Returns:
            List of QueryMetadata objects
        """
        ground_truths = []
        
        # Generate primary ground truth
        primary_gt = self.generate_ground_truth_ast(question)
        ground_truths.append(primary_gt)
        
        # Generate alternative ground truths if provided
        if alternative_queries:
            for alt_query in alternative_queries:
                try:
                    alt_gt = self.generate_ground_truth_ast(question, alt_query)
                    ground_truths.append(alt_gt)
                except Exception as e:
                    print(f"Warning: Failed to generate AST for alternative query: {str(e)}")
        
        return ground_truths
    
    def store_teacher_question(self, question: TeacherQuestion) -> int:
        """Store teacher question in database."""
        question_data = {
            'question_id': question.question_id,
            'question_text': question.question_text,
            'expected_sql': question.expected_sql,
            'rubric_criteria': question.rubric_criteria,
            'difficulty_level': question.difficulty_level,
            'tags': question.tags
        }
        return self.db_manager.store_teacher_question(question_data)
    
    def process_teacher_input(self, question: TeacherQuestion, 
                            ground_truth_queries: List[str] = None) -> Dict[str, Any]:
        """
        Complete processing of teacher input - stores question and generates ground truths.
        
        Args:
            question: Teacher question
            ground_truth_queries: List of correct SQL queries
            
        Returns:
            Dictionary with question_id and generated AST IDs
        """
        try:
            # Store teacher question
            question_id = self.store_teacher_question(question)
            
            # Generate and store ground truth ASTs
            ast_ids = []
            if ground_truth_queries:
                for sql_query in ground_truth_queries:
                    metadata = self.generate_ground_truth_ast(question, sql_query)
                    ast_id = self.store_ground_truth(metadata)
                    ast_ids.append(ast_id)
            else:
                # Generate from question text
                metadata = self.generate_ground_truth_ast(question)
                ast_id = self.store_ground_truth(metadata)
                ast_ids.append(ast_id)
            
            return {
                'question_id': question_id,
                'ground_truth_ast_ids': ast_ids,
                'status': 'success'
            }
            
        except Exception as e:
            return {
                'question_id': None,
                'ground_truth_ast_ids': [],
                'status': 'error',
                'error': str(e)
            }
    
    def _generate_sql_from_question(self, question: TeacherQuestion) -> str:
        """
        Generate SQL query from natural language question.
        This is a simplified implementation - in practice, you might use
        an LLM or more sophisticated NLP techniques.
        """
        # This is a placeholder implementation
        # In a real system, you would use NLP/LLM to convert question to SQL
        question_text = question.question_text.lower()
        
        # Simple keyword-based SQL generation (very basic example)
        if 'select' in question_text and 'from' in question_text:
            # Question already contains SQL-like structure
            return question_text
        elif 'count' in question_text:
            return "SELECT COUNT(*) FROM table_name"
        elif 'average' in question_text or 'avg' in question_text:
            return "SELECT AVG(column_name) FROM table_name"
        elif 'sum' in question_text:
            return "SELECT SUM(column_name) FROM table_name"
        elif 'maximum' in question_text or 'max' in question_text:
            return "SELECT MAX(column_name) FROM table_name"
        elif 'minimum' in question_text or 'min' in question_text:
            return "SELECT MIN(column_name) FROM table_name"
        else:
            # Default SELECT query
            return "SELECT * FROM table_name"
    
    def get_ground_truth_asts(self, question_id: str) -> List[QueryMetadata]:
        """Retrieve all ground truth ASTs for a question."""
        return self.db_manager.get_ground_truth_asts(question_id)
    
    def analyze_rubric_criteria(self, rubric_criteria: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze rubric criteria to understand evaluation requirements.
        
        Args:
            rubric_criteria: List of rubric criteria dictionaries
            
        Returns:
            Analysis of rubric requirements
        """
        analysis = {
            'total_criteria': len(rubric_criteria),
            'criteria_types': [],
            'weighted_criteria': [],
            'complexity_level': 'medium'
        }
        
        for criterion in rubric_criteria:
            criterion_type = criterion.get('type', 'unknown')
            analysis['criteria_types'].append(criterion_type)
            
            if criterion.get('weight', 0) > 0:
                analysis['weighted_criteria'].append(criterion)
        
        # Determine complexity level based on criteria
        if len(rubric_criteria) > 5 or any(c.get('complexity', 'medium') == 'high' for c in rubric_criteria):
            analysis['complexity_level'] = 'high'
        elif len(rubric_criteria) < 3:
            analysis['complexity_level'] = 'low'
        
        return analysis
    
    def validate_ground_truth_sql(self, sql_query: str) -> Dict[str, Any]:
        """
        Validate ground truth SQL query for syntax and structure.
        
        Args:
            sql_query: SQL query to validate
            
        Returns:
            Validation results
        """
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'suggestions': []
        }
        
        try:
            # Parse the SQL to check syntax
            ast_root = self.ast_parser.parse_sql_to_ast(sql_query)
            
            # Check for common issues
            if not ast_root.children:
                validation_result['warnings'].append("Query appears to be empty or invalid")
            
            # Check for required clauses
            has_select = any(child.node_type.value == 'SELECT' for child in ast_root.children)
            has_from = any(child.node_type.value == 'FROM' for child in ast_root.children)
            
            if not has_select:
                validation_result['errors'].append("Query missing SELECT clause")
            if not has_from:
                validation_result['warnings'].append("Query missing FROM clause")
            
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"SQL parsing error: {str(e)}")
        
        return validation_result