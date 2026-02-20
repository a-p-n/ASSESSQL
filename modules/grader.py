from sqlglot import exp
from sqlglot.optimizer.simplify import simplify
from typing import List, Dict, Any, Set
from .sql_processor import SQLProcessor

class Grader:
    def __init__(self, schema: Dict[str, Dict[str, str]]):
        self.processor = SQLProcessor(schema)

    def evaluate(self, student_sql: str, gt_sqls: List[str]) -> Dict[str, Any]:
        student_ast = self.processor.parse_and_optimize(student_sql)
        
        if not student_ast:
            return {
                "gts" : gt_sqls,
                "error": "Syntax Error or Invalid SQL Format.",
                "obtained_marks": 0,
                "total_marks": 0,
                "percentage": 0.0
            }

        student_ast = self._normalize_ast(student_ast)
        student_features = self._extract_features(student_ast)
        
        best_result = None
        best_score_ratio = -1.0

        for gt_sql in gt_sqls:
            gt_ast = self.processor.parse_and_optimize(gt_sql)
            if not gt_ast: continue

            gt_ast = self._normalize_ast(gt_ast)
            gt_features = self._extract_features(gt_ast)

            matches = student_features.intersection(gt_features)
            missing = gt_features - student_features
            extras = student_features - gt_features
            
            obtained = len(matches)
            total = len(gt_features)

            penalty = len(extras) * 0.5
            obtained = max(0.0, len(matches) - penalty)
            
            ratio = (obtained / total) if total > 0 else 0.0
            
            if ratio > best_score_ratio:
                best_score_ratio = ratio
                best_result = {
                    "matched_gt": gt_sql,
                    "obtained_marks": obtained,
                    "total_marks": total,
                    "percentage": round(ratio * 100, 2),
                    "feedback": {
                        "missing": list(missing),
                        "extras": list(extras)
                    }
                }

        return best_result

    def _normalize_ast(self, ast: exp.Expression) -> exp.Expression:
        ast = simplify(ast)

        def _standardize_commutative(node):
            if isinstance(node, (exp.EQ, exp.NEQ, exp.Or, exp.Add, exp.Mul)):
                left_str = node.left.sql()
                right_str = node.right.sql()
                if left_str > right_str:
                    return node.__class__(this=node.right, expression=node.left)
            return node

        def apply_equivalencies(node):
            if isinstance(node, exp.In) and not node.args.get("query"):
                if len(node.expressions) == 1:
                    new_node = exp.EQ(this=node.this, expression=node.expressions[0])
                    return _standardize_commutative(new_node)
            
            if isinstance(node, exp.Between):
                return exp.And(
                    this=exp.GTE(this=node.this, expression=node.args.get("lower")),
                    expression=exp.LTE(this=node.this, expression=node.args.get("upper"))
                )

            if isinstance(node, (exp.GT, exp.GTE, exp.LT, exp.LTE)):
                if isinstance(node.left, exp.Literal) and not isinstance(node.right, exp.Literal):
                    op_map = {exp.GT: exp.LT, exp.GTE: exp.LTE, exp.LT: exp.GT, exp.LTE: exp.GTE}
                    return op_map[type(node)](this=node.right, expression=node.left)

            if isinstance(node, exp.Count):
                if isinstance(node.this, exp.Literal) and str(node.this.this) == '1':
                    node.set("this", exp.Star())
                    return node

            return _standardize_commutative(node)

        return ast.transform(apply_equivalencies)

    def _extract_features(self, ast: exp.Expression) -> Set[str]:
        features = set()

        for table in ast.find_all(exp.Table):
            features.add(f"TABLE:{table.name}")

        for select in ast.find_all(exp.Select):
            for expression in select.expressions:
                features.add(f"SELECT:{expression.sql()}")

        def get_conditions(node):
            if isinstance(node, exp.And):
                yield from get_conditions(node.left)
                yield from get_conditions(node.right)
            else:
                yield node

        for where in ast.find_all(exp.Where):
            for condition in get_conditions(where.this):
                features.add(f"FILTER:{condition.sql()}")

        for join in ast.find_all(exp.Join):
            if join.args.get("on"):
                side = join.args.get("side") 
                prefix = "OUTER_JOIN_FILTER" if side else "FILTER"
                
                for condition in get_conditions(join.args.get("on")):
                    features.add(f"{prefix}:{condition.sql()}")

        for having in ast.find_all(exp.Having):
            for condition in get_conditions(having.this):
                features.add(f"HAVING:{condition.sql()}")

        for group in ast.find_all(exp.Group):
            for g in group.expressions:
                features.add(f"GROUP:{g.sql()}")

        for order in ast.find_all(exp.Order):
            for o in order.expressions:
                features.add(f"ORDER:{o.sql()}")

        for limit in ast.find_all(exp.Limit):
            if limit.expression:
                features.add(f"LIMIT:{limit.expression.sql()}")
        
        for col_def in ast.find_all(exp.ColumnDef):
            col_name = col_def.name
            data_type = col_def.args.get("kind").sql() if col_def.args.get("kind") else "UNKNOWN"
            features.add(f"COL_DEF:{col_name} TYPE:{data_type}")
            
            for constraint in col_def.find_all(exp.ColumnConstraint):
                features.add(f"CONSTRAINT:{col_name}->{constraint.sql()}")
                
        for constraint in ast.find_all(exp.Constraint):
            features.add(f"TABLE_CONSTRAINT:{constraint.sql()}")

        return features