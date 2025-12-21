# modules/__init__.py

# This allows: "from modules import DBManager" 
# Instead of: "from modules.db_manager import DBManager"

from .db_manager import DBManager
from .ingestion import IngestionModule
from .generator import GroundTruthGenerator
from .evaluator import HybridEvaluator