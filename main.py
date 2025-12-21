import os
import json
from config import Config

# Import our modular components
from modules import (
    IngestionModule,
    DBManager,
    GroundTruthGenerator,
    HybridEvaluator
)

def format_schema_for_llm(table_data):
    """
    Helper function to convert the JSON schema from the Ingestion Module 
    into a readable string for the LLM.
    """
    schema_context = ""
    for table in table_data:
        schema_context += f"Table Name: {table['table_name']}\n"
        if table.get('table_constraints'):
            schema_context += f"Constraints: {', '.join(table['table_constraints'])}\n"
        
        schema_context += "Columns:\n"
        for col in table['columns']:
            # Format: - column_name (DataType) [Constraints]
            c_str = f"  - {col['column_name']} ({col['datatype']})"
            if col.get('constraints'):
                c_str += f" [{col['constraints']}]"
            schema_context += c_str + "\n"
        schema_context += "\n"
    return schema_context

def main():
    print("==================================================")
    print("   AssessSQL: Automated Assessment Pipeline")
    print("==================================================")

    # ---------------------------------------------------------
    # 1. INITIALIZATION
    # ---------------------------------------------------------
    print("\n[INIT] Initializing Modules...")
    
    # Check if we have the PDF to process
    lab_manual_path = "lab1.pdf"  # Make sure this file exists
    if not os.path.exists(lab_manual_path):
        print(f"[ERROR] '{lab_manual_path}' not found. Please place it in the project root.")
        return

    # Initialize components
    # DB Manager connects to the Sandbox DB for execution checks
    db_manager = DBManager(Config.DB_PATH) 
    
    # Ingestion handles the OCR and Parsing
    ingestion = IngestionModule()
    
    # Generator handles the LLM (loading model to GPU/CPU)
    # We wrap this in a try-catch in case model weights aren't downloaded yet
    try:
        generator = GroundTruthGenerator(Config.MODEL_NAME, Config.DEVICE)
    except Exception as e:
        print(f"[ERROR] Failed to load LLM: {e}")
        return

    # Evaluator compares Student vs Gold
    evaluator = HybridEvaluator(db_manager)

    # ---------------------------------------------------------
    # 2. LEVEL 1: TEACHER FLOW (Setup & Ground Truth)
    # ---------------------------------------------------------
    print("\n[LEVEL 1] Starting Teacher Flow (Ingestion & Gold Generation)...")
    
    # A. Ingest the Lab Manual
    # This returns a list of scenarios (tables + questions) found in the PDF
    lab_scenarios = ingestion.process_file(lab_manual_path)
    
    if not lab_scenarios:
        print("[ERROR] No data extracted from PDF. Exiting.")
        return

    # We will store the generated "Gold Standard" data here to use in Level 2
    gold_standard_registry = []

    for scenario_idx, scenario in enumerate(lab_scenarios):
        q_id = scenario.get('question_id', scenario_idx)
        print(f"\n--- Processing Scenario/Question Set #{q_id} ---")
        
        # B. Format Schema for the LLM
        # The LLM needs text, not JSON, to understand the database structure
        schema_context_str = format_schema_for_llm(scenario['tables'])
        
        # C. Generate Gold SQL for each question in this scenario
        queries = scenario.get('queries', [])
        for q_idx, natural_question in enumerate(queries):
            print(f"   > Processing Question {q_idx + 1}: '{natural_question[:40]}...'")
            
            # Generate the solution
            gold_sql = generator.generate_gold_query(natural_question, schema_context_str)
            
            # Store it for Level 2
            gold_entry = {
                "scenario_id": q_id,
                "question_text": natural_question,
                "schema_context": schema_context_str,
                "gold_sql": gold_sql
            }
            gold_standard_registry.append(gold_entry)
            
            print(f"     [Gold SQL Generated]: {gold_sql}")

    # ---------------------------------------------------------
    # 3. LEVEL 2: STUDENT FLOW (Evaluation Demo)
    # ---------------------------------------------------------
    print("\n\n[LEVEL 2] Starting Student Flow (Evaluation Demo)...")
    
    if not gold_standard_registry:
        print("[WARN] No gold standards generated. Cannot evaluate students.")
        return

    # Let's pick the first generated problem to test
    test_case = gold_standard_registry[0]
    
    print(f"Target Question: {test_case['question_text']}")
    print(f"Reference (Gold) SQL: {test_case['gold_sql']}")
    
    # --- SIMULATED STUDENT SUBMISSIONS ---
    
    # Submission 1: Exact Match
    student_sub_1 = test_case['gold_sql'] 
    
    # Submission 2: Syntactically different but logically same (e.g., lower case)
    student_sub_2 = test_case['gold_sql'].lower()
    
    # Submission 3: Completely Wrong
    student_sub_3 = "SELECT * FROM wrong_table"

    submissions = [student_sub_1, student_sub_2, student_sub_3]
    
    for i, sub in enumerate(submissions):
        print(f"\n--- Evaluating Student Submission {i+1} ---")
        print(f"Student Query: {sub}")
        
        # Run Evaluation
        report = evaluator.evaluate(
            student_sql=sub,
            gold_sql=test_case['gold_sql']
        )
        
        # Display Results
        print("Evaluation Report:")
        print(f"  > Structural Match (AST): {report['structural_match']}")
        print(f"  > Execution Match:        {report['execution_match']}")
        print(f"  > Final Score:            {report['score']}/100")
        
        if 'execution_error' in report:
            print(f"  > Execution Error:        {report['execution_error']}")

    print("\n==================================================")
    print("   Pipeline Execution Complete")
    print("==================================================")

if __name__ == "__main__":
    main()