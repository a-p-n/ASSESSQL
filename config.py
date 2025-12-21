# config.py
import torch

class Config:
    # System Device
    DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # Models (Using the ones you are experimenting with)
    MODEL_NAME = "suriya7/t5-base-text-to-sql" 
    # MODEL_NAME = "suriya7/t5-base-text-to-sql"
    
    # Database (The sandbox DB where queries are tested)
    DB_PATH = "sqlite:///student_test.db"  # Example SQLite path
    
    # Rubrics
    DEFAULT_PASSING_SCORE = 70.0