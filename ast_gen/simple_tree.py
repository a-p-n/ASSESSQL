import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from ast_parser import SQLASTParser
    parser = SQLASTParser()
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

def print_simple_tree(ast_dict, indent=0, is_last=True, prefix=""):
    
    if indent == 0:
        current_prefix = ""
        next_prefix = ""
    else:
        if is_last:
            current_prefix = prefix + "└─ "
            next_prefix = prefix + "   "
        else:
            current_prefix = prefix + "├─ "
            next_prefix = prefix + "│  "
    
    node_type = ast_dict.get('node_type', 'UNKNOWN')
    value = ast_dict.get('value', '')
    
    if value and value != node_type:
        print(f"{current_prefix}{node_type}: {value}")
    else:
        print(f"{current_prefix}{node_type}")
    
    children = ast_dict.get('children', [])
    for i, child in enumerate(children):
        is_last_child = (i == len(children) - 1)
        print_simple_tree(child, indent + 1, is_last_child, next_prefix)

def main():
    
    if len(sys.argv) < 2:
        print("Usage: python simple_tree.py \"SQL_QUERY\"")
        print("Example: python simple_tree.py \"SELECT * FROM users WHERE age > 18\"")
        sys.exit(1)
    
    sql_query = " ".join(sys.argv[1:])
    
    try:
        print(f"SQL: {sql_query}")
        print("\nTree Structure:")
        print("-" * 30)
        
        ast = parser.parse_sql_to_ast(sql_query)
        ast_dict = parser.ast_to_dict(ast)
        print_simple_tree(ast_dict)
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()