from sqlglot import exp, TokenType, tokenize, parse_one
from sqlglot.optimizer.qualify import qualify
from typing import Dict, Any, Optional
from collections import defaultdict

class SQLProcessor:
    def __init__(self, schema: Dict[str, Dict[str, str]]):
        self.schema = schema

    def parse_and_optimize(self, sql_query: str) -> Optional[exp.Expression]:
        try:
            self._validate_structure(sql_query)
            expression = parse_one(sql_query, read="postgres")
            
            expression = self._normalize_casing(expression)

            expression = qualify(
                expression, 
                schema=self.schema,
                quote_identifiers=False,
                validate_qualify_columns=False
            )

            expression = self._standardize_aliases(expression)
            
            return expression

        except Exception as e:
            print(f"SQL Processing Error for '{sql_query}': {e}")
            return None

    def _validate_structure(self, sql_query: str):
        tokens = tokenize(sql_query)
        if not tokens:
            return

        first_token = tokens[0]
        
        if first_token.token_type not in [TokenType.SELECT, TokenType.UPDATE, TokenType.INSERT, TokenType.DELETE, TokenType.ALTER, TokenType.DROP]:
            raise ValueError("Invalid SQL Syntax.")

    def _normalize_casing(self, expression: exp.Expression) -> exp.Expression:
        def transform(node):
            if isinstance(node, exp.Identifier):
                node.set("this", node.this.lower())
                node.set("quoted", False)
            return node
        return expression.transform(transform)

    def _standardize_aliases(self, expression: exp.Expression) -> exp.Expression:
        table_counts = defaultdict(int)
        for table in expression.find_all(exp.Table):
            table_counts[table.name.lower()] += 1
            
        alias_map = {}
        
        for table in expression.find_all(exp.Table):
            real_name = table.name.lower()
            current_alias = table.alias.lower() if table.alias else ""
            
            if table_counts[real_name] == 1:
                if current_alias and current_alias != real_name:
                    alias_map[current_alias] = real_name
                    
                    new_alias = exp.TableAlias(this=exp.Identifier(this=real_name, quoted=False))
                    table.set("alias", new_alias)

        if alias_map:
            for col in expression.find_all(exp.Column):
                if col.table:
                    t_ref = col.table.lower()
                    if t_ref in alias_map:
                        col.set("table", exp.Identifier(this=alias_map[t_ref], quoted=False))

        return expression

    def get_canonical_sql(self, expression: exp.Expression) -> str:
        return expression.sql()