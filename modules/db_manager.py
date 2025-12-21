# modules/db_manager.py
from sqlalchemy import create_engine, inspect, text

class DBManager:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.inspector = inspect(self.engine)

    def get_schema_context(self):
        """
        Retrieves table names and columns to feed into the LLM.
        Replaces the GNN's 'feature extraction' role for now.
        """
        schema_text = ""
        for table_name in self.inspector.get_table_names():
            columns = self.inspector.get_columns(table_name)
            col_str = ", ".join([f"{col['name']} ({col['type']})" for col in columns])
            schema_text += f"Table {table_name}: {col_str}\n"
        return schema_text

    def execute_query(self, sql_query):
        """Safe execution of queries for validation."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql_query))
                return result.fetchall()
        except Exception as e:
            return str(e)