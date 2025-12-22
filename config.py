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