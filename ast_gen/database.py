from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import JSON
from datetime import datetime
from typing import List, Optional, Dict, Any
import json

from .models import QueryMetadata, ASTNode
from config import DatabaseConfig

Base = declarative_base()

class QueryMetadataDB(Base):
    __tablename__ = 'query_metadata'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    query_text = Column(Text, nullable=False)
    ast_json = Column(JSON, nullable=False)
    question_id = Column(String(255))
    rubric_id = Column(String(255))
    student_id = Column(String(255))
    is_ground_truth = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    score = Column(Float)
    feedback = Column(Text)

class TeacherQuestionDB(Base):
    __tablename__ = 'teacher_questions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    question_id = Column(String(255), unique=True, nullable=False)
    question_text = Column(Text, nullable=False)
    expected_sql = Column(Text)
    rubric_criteria = Column(JSON)
    difficulty_level = Column(String(50), default='medium')
    tags = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class StudentSubmissionDB(Base):
    __tablename__ = 'student_submissions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    student_id = Column(String(255), nullable=False)
    question_id = Column(String(255), nullable=False)
    sql_query = Column(Text, nullable=False)
    submission_time = Column(DateTime, default=datetime.utcnow)
    ast_json = Column(JSON)
    score = Column(Float)
    feedback = Column(Text)

class DatabaseManager:
    
    def __init__(self):
        self.config = DatabaseConfig()
        self.engine = create_engine(self.config.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
    def create_tables(self):
        Base.metadata.create_all(bind=self.engine)
        
    def get_session(self):
        return self.SessionLocal()
    
    def store_ground_truth_ast(self, query_metadata: QueryMetadata) -> int:
        session = self.get_session()
        try:
            db_record = QueryMetadataDB(
                query_text=query_metadata.query_text,
                ast_json=query_metadata.ast_json,
                question_id=query_metadata.question_id,
                rubric_id=query_metadata.rubric_id,
                is_ground_truth=True,
                created_at=datetime.utcnow()
            )
            session.add(db_record)
            session.commit()
            return db_record.id
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def store_student_ast(self, query_metadata: QueryMetadata) -> int:
        session = self.get_session()
        try:
            db_record = QueryMetadataDB(
                query_text=query_metadata.query_text,
                ast_json=query_metadata.ast_json,
                student_id=query_metadata.student_id,
                question_id=query_metadata.question_id,
                is_ground_truth=False,
                created_at=datetime.utcnow()
            )
            session.add(db_record)
            session.commit()
            return db_record.id
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_ground_truth_asts(self, question_id: str) -> List[QueryMetadata]:
        session = self.get_session()
        try:
            records = session.query(QueryMetadataDB).filter(
                QueryMetadataDB.question_id == question_id,
                QueryMetadataDB.is_ground_truth == True
            ).all()
            
            return [
                QueryMetadata(
                    id=record.id,
                    query_text=record.query_text,
                    ast_json=record.ast_json,
                    question_id=record.question_id,
                    rubric_id=record.rubric_id,
                    is_ground_truth=record.is_ground_truth,
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                    score=record.score,
                    feedback=record.feedback
                )
                for record in records
            ]
        finally:
            session.close()
    
    def get_student_asts(self, student_id: str, question_id: str) -> List[QueryMetadata]:
        session = self.get_session()
        try:
            records = session.query(QueryMetadataDB).filter(
                QueryMetadataDB.student_id == student_id,
                QueryMetadataDB.question_id == question_id,
                QueryMetadataDB.is_ground_truth == False
            ).all()
            
            return [
                QueryMetadata(
                    id=record.id,
                    query_text=record.query_text,
                    ast_json=record.ast_json,
                    student_id=record.student_id,
                    question_id=record.question_id,
                    is_ground_truth=record.is_ground_truth,
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                    score=record.score,
                    feedback=record.feedback
                )
                for record in records
            ]
        finally:
            session.close()
    
    def store_teacher_question(self, question_data: Dict[str, Any]) -> int:
        session = self.get_session()
        try:
            db_record = TeacherQuestionDB(
                question_id=question_data['question_id'],
                question_text=question_data['question_text'],
                expected_sql=question_data.get('expected_sql'),
                rubric_criteria=question_data.get('rubric_criteria'),
                difficulty_level=question_data.get('difficulty_level', 'medium'),
                tags=question_data.get('tags', []),
                created_at=datetime.utcnow()
            )
            session.add(db_record)
            session.commit()
            return db_record.id
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def store_student_submission(self, submission_data: Dict[str, Any]) -> int:
        session = self.get_session()
        try:
            db_record = StudentSubmissionDB(
                student_id=submission_data['student_id'],
                question_id=submission_data['question_id'],
                sql_query=submission_data['sql_query'],
                submission_time=submission_data.get('submission_time', datetime.utcnow()),
                ast_json=submission_data.get('ast_json'),
                score=submission_data.get('score'),
                feedback=submission_data.get('feedback')
            )
            session.add(db_record)
            session.commit()
            return db_record.id
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def update_query_score(self, query_id: int, score: float, feedback: str = None):
        session = self.get_session()
        try:
            record = session.query(QueryMetadataDB).filter(QueryMetadataDB.id == query_id).first()
            if record:
                record.score = score
                if feedback:
                    record.feedback = feedback
                record.updated_at = datetime.utcnow()
                session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_all_questions(self) -> List[Dict[str, Any]]:
        session = self.get_session()
        try:
            records = session.query(TeacherQuestionDB).all()
            return [
                {
                    'id': record.id,
                    'question_id': record.question_id,
                    'question_text': record.question_text,
                    'expected_sql': record.expected_sql,
                    'rubric_criteria': record.rubric_criteria,
                    'difficulty_level': record.difficulty_level,
                    'tags': record.tags,
                    'created_at': record.created_at,
                    'updated_at': record.updated_at
                }
                for record in records
            ]
        finally:
            session.close()
    
    def get_question_by_id(self, question_id: str) -> Optional[Dict[str, Any]]:
        session = self.get_session()
        try:
            record = session.query(TeacherQuestionDB).filter(
                TeacherQuestionDB.question_id == question_id
            ).first()
            
            if record:
                return {
                    'id': record.id,
                    'question_id': record.question_id,
                    'question_text': record.question_text,
                    'expected_sql': record.expected_sql,
                    'rubric_criteria': record.rubric_criteria,
                    'difficulty_level': record.difficulty_level,
                    'tags': record.tags,
                    'created_at': record.created_at,
                    'updated_at': record.updated_at
                }
            return None
        finally:
            session.close()