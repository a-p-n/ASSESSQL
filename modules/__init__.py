# modules/__init__.py

# This allows: "from modules import DBManager" 
# Instead of: "from modules.db_manager import DBManager"

from modules.db_manager import DBManager
from modules.ingestion import IngestionPipeline
from modules.generator import SQLGenerator
from modules.evaluator import HybridEvaluator