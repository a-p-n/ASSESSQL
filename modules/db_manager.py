import sqlite3
import os

class DBManager:
    def __init__(self, db_dir):
        self.db_dir = db_dir
        self.current_db_path = None

    def set_question_db(self, db_id):
        self.current_db_path = os.path.join(self.db_dir, db_id, f"{db_id}.sqlite")
        if not os.path.exists(self.current_db_path):
            return False, f"Database file not found: {self.current_db_path}"
        return True, None

    def get_schema_context(self, db_id):
        success, error = self.set_question_db(db_id)
        if not success: return error
        
        schema_text = ""
        try:
            conn = sqlite3.connect(self.current_db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                col_str = ", ".join([f"{col[1]} ({col[2]})" for col in columns])
                schema_text += f"Table {table_name}: {col_str}\n"
            
            conn.close()
            return schema_text
        except Exception as e:
            return f"Error reading schema: {str(e)}"

    def execute_query(self, sql_query, db_id):
        self.set_question_db(db_id)
        try:
            conn = sqlite3.connect(f"file:{self.current_db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            conn.close()
            return result
        except Exception as e:
            return f"Error: {str(e)}"