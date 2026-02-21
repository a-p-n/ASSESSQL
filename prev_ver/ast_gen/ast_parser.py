import sqlparse
from sqlparse import sql, tokens
from typing import List, Dict, Any, Optional

try:
    from ast_gen.models import ASTNode, ASTNodeType
except ImportError:
    from .models import ASTNode, ASTNodeType

class SQLASTParser:
    def parse_sql_to_ast(self, sql_query: str) -> ASTNode:
        clean_sql = sql_query.strip().rstrip(';')
        parsed = sqlparse.parse(clean_sql)
        if not parsed:
            raise ValueError("Empty or invalid SQL query")
        return self._parse_statement(parsed[0])
    
    def _parse_statement(self, statement) -> ASTNode:
        root = ASTNode(node_type=ASTNodeType.SELECT, value="QUERY")
        
        self._attach_clause(root, statement, 'SELECT', ASTNodeType.SELECT, self._parse_columns)
        self._attach_clause(root, statement, 'FROM', ASTNodeType.FROM, self._parse_tables)
        self._attach_clause(root, statement, 'WHERE', ASTNodeType.WHERE, self._parse_conditions)
        self._attach_clause(root, statement, 'GROUP BY', ASTNodeType.GROUP_BY, self._parse_columns)
        self._attach_clause(root, statement, 'HAVING', ASTNodeType.HAVING, self._parse_conditions)
        self._attach_clause(root, statement, 'ORDER BY', ASTNodeType.ORDER_BY, self._parse_columns)
        
        return root

    def _attach_clause(self, root, statement, keyword, node_type, parser_func):
        tokens_found = self._extract_tokens(statement, keyword)
        if tokens_found:
            node = ASTNode(node_type=node_type, value=keyword)
            children = parser_func(tokens_found)
            node.children.extend(children)
            root.children.append(node)

    def _extract_tokens(self, statement, keyword) -> List:
        tokens_list = []
        in_clause = False
        key_upper = keyword.upper()
        
        for token in statement.flatten():
            if token.ttype in [tokens.Whitespace, tokens.Newline]:
                continue
            val = token.value.upper()
            
            # Start of Clause
            if token.ttype is tokens.Keyword and val == key_upper.split()[0]:
                in_clause = True
                continue
            
            # End of Clause (hitting next keyword)
            if token.ttype is tokens.Keyword and val in ['FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT']:
                if val != key_upper.split()[0]:
                    in_clause = False
            
            if in_clause:
                tokens_list.append(token)
        return tokens_list

    def _parse_columns(self, tokens_list) -> List[ASTNode]:
        nodes = []
        current = []
        for token in tokens_list:
            if token.value == ',':
                if current: nodes.append(self._create_column_node(current))
                current = []
            else:
                current.append(token)
        if current: nodes.append(self._create_column_node(current))
        return nodes

    def _create_column_node(self, tokens_list) -> ASTNode:
        # Join tokens to handle cases like "t" "." "col" or "t.col"
        text = ''.join([str(t) for t in tokens_list]).strip()
        
        # Handle Functions e.g. COUNT(*)
        if '(' in text and ')' in text:
            return ASTNode(node_type=ASTNodeType.FUNCTION, value=text.upper())
            
        table_ref = None
        col_ref = text
        
        # Explicit splitting by dot for alias handling
        if '.' in text:
            parts = text.split('.')
            if len(parts) == 2:
                table_ref = parts[0]
                col_ref = parts[1]
        
        # Clean quotes if present
        col_ref = col_ref.replace('"', '').replace("'", "")
        if table_ref: table_ref = table_ref.replace('"', '').replace("'", "")

        meta = {"column_name": col_ref}
        if table_ref: meta["table_ref"] = table_ref
        
        return ASTNode(node_type=ASTNodeType.COLUMN, value=col_ref, metadata=meta)

    def _parse_tables(self, tokens_list) -> List[ASTNode]:
        nodes = []
        current = []
        for token in tokens_list:
            if token.value == ',':
                if current: nodes.append(self._create_table_node(current))
                current = []
            else:
                current.append(token)
        if current: nodes.append(self._create_table_node(current))
        return nodes

    def _create_table_node(self, tokens_list) -> ASTNode:
        text = ''.join([str(t) for t in tokens_list]).strip()
        parts = text.upper().replace(' AS ', ' ').split()
        
        # "EMP E" -> table: EMP, alias: E
        table_name = parts[0]
        alias = parts[1] if len(parts) > 1 else None
        
        meta = {"table_name": table_name}
        if alias: meta["alias"] = alias
        
        return ASTNode(node_type=ASTNodeType.TABLE, value=text, metadata=meta)

    def _parse_conditions(self, tokens_list) -> List[ASTNode]:
        nodes = []
        curr = []
        for token in tokens_list:
            if token.value.upper() in ['AND', 'OR']:
                if curr: nodes.append(ASTNode(node_type=ASTNodeType.CONDITION, value=''.join(str(t) for t in curr)))
                nodes.append(ASTNode(node_type=ASTNodeType.OPERATOR, value=token.value.upper()))
                curr = []
            else:
                curr.append(token)
        if curr: nodes.append(ASTNode(node_type=ASTNodeType.CONDITION, value=''.join(str(t) for t in curr)))
        return nodes