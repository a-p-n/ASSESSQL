# config.py
import torch
import os

# --- System Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# INPUT: Your Lab Manual PDF
PDF_PATH = os.path.join(BASE_DIR, "lab1.pdf") 

# OUTPUT: Where data will be saved
DATA_DIR = os.path.join(BASE_DIR, "data")
SCHEMA_DIR = os.path.join(DATA_DIR, "databases")
DATASET_PATH = os.path.join(DATA_DIR, "assessql_custom_dataset.json")

# --- Model Settings ---
# Note: T5 is a Seq2Seq model, different from SQLCoder
MODEL_ID = "suriya7/t5-base-text-to-sql" 
MODEL_TYPE = "seq2seq"  # New flag to help main.py know how to load it

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# --- Generation Settings ---
MAX_NEW_TOKENS = 512
NUM_BEAMS = 5