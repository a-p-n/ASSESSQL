from modules.grader import Grader

def run_test():
    schema = {
        "emp": {"eno": "varchar", "ename": "varchar", "dept_no": "varchar", "salary": "int"}
    }
    
    grader = Grader(schema)
    
    gt_queries = [
        "SELECT e1.eno, e1.ename, e2.eno, e2.ename, e1.dept_no FROM emp e1 INNER JOIN emp e2 ON e1.dept_no = e2.dept_no WHERE e1.eno <> e2.eno;",
        "SELECT e1.eno, e1.ename, e2.eno, e2.ename, e1.dept_no FROM emp e1 JOIN emp e2 ON e1.dept_no = e2.dept_no WHERE e1.eno <> e2.eno;",
        "SELECT e1.eno, e1.ename, e1.dept_no FROM emp e1 WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e1.dept_no = e2.dept_no AND e1.eno <> e2.eno);",
        "SELECT eno, ename, dept_no FROM emp WHERE dept_no IN (SELECT dept_no FROM emp GROUP BY dept_no HAVING COUNT(*) > 1);"
    ]

    print("--- SCENARIO 1 ---")
    student_1 = """SELECT e1.eno, e1.ename, e2.eno, e2.ename, e1.dept_no FROM emp e1 INNER JOIN emp e2 ON e1.dept_no = e2.dept_no WHERE e1.eno <> e2.eno"""
    result_1 = grader.evaluate(student_1, gt_queries)
    print(f"Query : {student_1}")
    print(f"Result : {result_1}")


    print("\n--- SCENARIO 2 ---")
    student_2 = """SELECT e1.eno, e1.ename, e2.eno, e2.ename, e1.dept_no FROM emp e1 JOIN emp e2 ON e1.dept_no = e2.dept_no WHERE e1.eno <> e2.eno"""
    result_2 = grader.evaluate(student_2, gt_queries)
    print(f"Query : {student_2}")
    print(f"Result : {result_2}")


    print("\n--- SCENARIO 3 ---")
    student_3 = """SELECT e1.eno, e1.ename, e2.eno, e2.ename, e1.dept_no FROM emp e1, emp e2 WHERE e1.dept_no = e2.dept_no AND e1.eno <> e2.eno"""
    result_3 = grader.evaluate(student_3, gt_queries)
    print(f"Query : {student_3}")
    print(f"Result : {result_3}")


    print("\n--- SCENARIO 4 ---")
    student_4 = """SELECT e1.eno, e1.ename, e1.dept_no FROM emp e1 WHERE EXISTS (SELECT 1 FROM emp e2 WHERE e1.dept_no = e2.dept_no AND e1.eno <> e2.eno)"""
    result_4 = grader.evaluate(student_4, gt_queries)
    print(f"Query : {student_4}")
    print(f"Result : {result_4}")


    print("\n--- SCENARIO 5 ---")
    student_5 = """SELECT eno, ename FROM emp WHERE dept_no = 'D1'"""
    result_5 = grader.evaluate(student_5, gt_queries)
    print(f"Query : {student_5}")
    print(f"Result : {result_5}")

if __name__ == "__main__":
    run_test()