from modules.sql_processor import SQLProcessor

def run_test():
    schema = {
        "emp": {
            "eno": "varchar",
            "ename": "varchar",
            "basic-sal": "integer",
            "dept_no": "varchar"
        },
        "dept": {
            "dno": "varchar",
            "dname": "varchar"
        }
    }

    processor = SQLProcessor(schema)

    print("--- TEST SCENARIO ---")

    query_1 = "SELECT eno, ename FROM emp WHERE dept_no = 'D1'"
    query_2 = "SELECT e.eno, ename FROM emp e"
    query_3 = "from EMP f SELECT f.ENO, f.Ename where f.DEPT_NO = 'D1'"

    print(f"Query 1:\t{query_1}")
    print(f"Query 2:\t{query_2}")
    print(f"Query 3:\t{query_3}")

    ast_1 = processor.parse_and_optimize(query_1)
    ast_2 = processor.parse_and_optimize(query_2)
    ast_3 = processor.parse_and_optimize(query_3)

    if not (ast_1 and ast_2 and ast_3):
        print("Error parsing queries.")
        return

    canon_1 = processor.get_canonical_sql(ast_1)
    canon_2 = processor.get_canonical_sql(ast_2)
    canon_3 = processor.get_canonical_sql(ast_3)

    print(f"\nCanonical 1: {canon_1}")
    print(f"Canonical 2: {canon_2}")
    print(f"Canonical 3: {canon_3}")

    print("\n--- VERIFICATION ---")
    if canon_1 == canon_2 == canon_3:
        print("SUCCESS! All queries normalized to the exact same structure.")
    else:
        print("FAILED. The normalizer didn't converge.")

if __name__ == "__main__":
    run_test()