import torch
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PDF_PATH = os.path.join(BASE_DIR, "lab1.pdf") 

DATA_DIR = os.path.join(BASE_DIR, "data")
SCHEMA_DIR = os.path.join(DATA_DIR, "databases")
DATASET_PATH = os.path.join(DATA_DIR, "assessql_custom_dataset.json")

MODEL_ID = "suriya7/t5-base-text-to-sql" 
MODEL_TYPE = "seq2seq"

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

MAX_NEW_TOKENS = 512
NUM_BEAMS = 5

import os

class DatabaseConfig:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "assessql")
    DB_USER = os.getenv("DB_USER", "username")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

class AppConfig:
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
