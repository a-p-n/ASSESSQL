from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

class ASTNodeType(str, Enum):
    QUERY = "QUERY"
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    TRUNCATE = "TRUNCATE"
    
    FROM = "FROM"
    WHERE = "WHERE"
    JOIN = "JOIN"
    INNER_JOIN = "INNER_JOIN"
    LEFT_JOIN = "LEFT_JOIN"
    RIGHT_JOIN = "RIGHT_JOIN"
    FULL_JOIN = "FULL_JOIN"
    CROSS_JOIN = "CROSS_JOIN"
    ON = "ON"
    GROUP_BY = "GROUP_BY"
    HAVING = "HAVING"
    ORDER_BY = "ORDER_BY"
    LIMIT = "LIMIT"
    OFFSET = "OFFSET"
    
    UNION = "UNION"
    UNION_ALL = "UNION_ALL"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"
    
    WITH = "WITH"
    CTE = "CTE"
    
    VALUES = "VALUES"
    SET = "SET"
    INTO = "INTO"
    
    COLUMN_DEF = "COLUMN_DEF"
    CONSTRAINT = "CONSTRAINT"
    PRIMARY_KEY = "PRIMARY_KEY"
    FOREIGN_KEY = "FOREIGN_KEY"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"
    INDEX = "INDEX"
    
    COLUMN = "COLUMN"
    TABLE = "TABLE"
    CONDITION = "CONDITION"
    FUNCTION = "FUNCTION"
    OPERATOR = "OPERATOR"
    LITERAL = "LITERAL"
    SUBQUERY = "SUBQUERY"
    ALIAS = "ALIAS"
    EXPRESSION = "EXPRESSION"
    CASE = "CASE"
    WHEN = "WHEN"
    THEN = "THEN"
    ELSE = "ELSE"
    END = "END"

class ASTNode(BaseModel):
    node_type: ASTNodeType
    value: Optional[str] = None
    children: List['ASTNode'] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    position: Optional[Dict[str, int]] = None
    
    class Config:
        use_enum_values = True

class QueryMetadata(BaseModel):
    id: Optional[int] = None
    query_text: str
    ast_json: Dict[str, Any]
    question_id: Optional[str] = None
    rubric_id: Optional[str] = None
    student_id: Optional[str] = None
    is_ground_truth: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    score: Optional[float] = None
    feedback: Optional[str] = None

class TeacherQuestion(BaseModel):
    question_id: str
    question_text: str
    expected_sql: Optional[str] = None
    rubric_criteria: List[Dict[str, Any]]
    difficulty_level: str = "medium"
    tags: List[str] = Field(default_factory=list)

class StudentSubmission(BaseModel):
    student_id: str
    question_id: str
    sql_query: str
    submission_time: Optional[datetime] = None

class ASTComparisonResult(BaseModel):
    similarity_score: float
    matching_nodes: int
    total_nodes: int
    differences: List[Dict[str, Any]]
    feedback: List[str]