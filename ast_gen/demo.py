import json
from student_module import StudentQueryProcessor
from ast_parser import SQLASTParser
from simple_tree import print_simple_tree

def run_visual_evaluation(student_sql, ground_truths, rubric):
    processor = StudentQueryProcessor()
    parser = SQLASTParser()
    
    print("="*60)
    print("      ASSESSQL: LOGICAL AST VISUALIZATION")
    print("="*60)

    print("\n[STUDENT QUERY]:", student_sql)
    student_ast_root = parser.parse_sql_to_ast(student_sql)
    student_dict = parser.ast_to_dict(student_ast_root)
    print_simple_tree(student_dict)

    print("\n" + "-"*60)
    print("[GROUND TRUTH LIBRARY]")
    gt_dicts = []
    for i, gt_sql in enumerate(ground_truths):
        print(f"\nGround Truth #{i+1}: {gt_sql}")
        gt_root = parser.parse_sql_to_ast(gt_sql)
        gt_dict = parser.ast_to_dict(gt_root)
        gt_dicts.append(gt_dict)
        print_simple_tree(gt_dict)

    print("\n" + "="*60)
    print("      EVALUATION RESULTS")
    print("="*60)
    
    result = processor.evaluate_submission(student_sql, ground_truths, rubric)
    
    print(f"\nLOGICAL SIMILARITY: {result['similarity_score'] * 100}%")
    print(f"RUBRIC GRADE:       {result['final_grade']} / 100")
    
    print("\n[PEDAGOGICAL FEEDBACK]:")
    for msg in result['feedback']:
        print(f"  > {msg}")

    print("\n[STRUCTURAL DIFFERENCES DETECTED]:")
    if not result['differences']:
        print("  None - Perfect logical match.")
    else:
        for diff in result['differences']:
            print(f"{diff}")

teacher_rubric = {
    "projections": 20,
    "tables": 30,
    "filters": 50
}

gt_list = [
    "SELECT Eno FROM EMP WHERE incentive > 5000",
    "SELECT Eno FROM EMP WHERE 5000 < incentive"
]

student_input = "SELECT Eno FROM EMP"

run_visual_evaluation(student_input, gt_list, teacher_rubric)