import sqlparse
from sqlparse import sql, tokens
from typing import List, Dict, Any, Optional, Tuple
from models import ASTNode, ASTNodeType

class SQLASTParser:
    def __init__(self):
        self.operators = {
            '=', '!=', '<>', '<', '>', '<=', '>=', 
            'AND', 'OR', 'NOT', 'IN', 'NOT IN', 'LIKE', 'NOT LIKE',
            'BETWEEN', 'IS NULL', 'IS NOT NULL'
        }
        
    def parse_sql_to_ast(self, sql_query: str) -> ASTNode:
        try:
            parsed = sqlparse.parse(sql_query.strip())
            if not parsed:
                raise ValueError("Empty or invalid SQL query")
                
            statement = parsed[0]
            return self._parse_statement(statement)
            
        except Exception as e:
            raise ValueError(f"Failed to parse SQL query: {str(e)}")
    
    def _parse_statement(self, statement) -> ASTNode:
        root = ASTNode(
            node_type=ASTNodeType.SELECT,
            value="QUERY",
            metadata={"query_type": "SELECT"}
        )
        
        select_part = self._extract_select_part(statement)
        from_part = self._extract_from_part(statement)
        where_part = self._extract_where_part(statement)
        group_by_part = self._extract_group_by_part(statement)
        having_part = self._extract_having_part(statement)
        order_by_part = self._extract_order_by_part(statement)
        limit_part = self._extract_limit_part(statement)
        
        if select_part:
            root.children.append(select_part)
        if from_part:
            root.children.append(from_part)
        if where_part:
            root.children.append(where_part)
        if group_by_part:
            root.children.append(group_by_part)
        if having_part:
            root.children.append(having_part)
        if order_by_part:
            root.children.append(order_by_part)
        if limit_part:
            root.children.append(limit_part)
            
        return root
    
    def _extract_select_part(self, statement) -> Optional[ASTNode]:
        select_tokens = []
        in_select = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() == 'SELECT':
                in_select = True
                continue
            elif token.ttype is tokens.Keyword and token.value.upper() in ['FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT']:
                break
            elif in_select and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                select_tokens.append(token)
        
        if not select_tokens:
            return None
            
        select_node = ASTNode(
            node_type=ASTNodeType.SELECT,
            value="SELECT",
            metadata={"clause_type": "SELECT"}
        )
        
        columns = self._parse_columns(select_tokens)
        for column in columns:
            select_node.children.append(column)
            
        return select_node
    
    def _extract_from_part(self, statement) -> Optional[ASTNode]:
        from_tokens = []
        in_from = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() == 'FROM':
                in_from = True
                continue
            elif token.ttype is tokens.Keyword and token.value.upper() in ['WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT']:
                break
            elif in_from and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                from_tokens.append(token)
        
        if not from_tokens:
            return None
            
        from_node = ASTNode(
            node_type=ASTNodeType.FROM,
            value="FROM",
            metadata={"clause_type": "FROM"}
        )
        
        tables = self._parse_tables(from_tokens)
        for table in tables:
            from_node.children.append(table)
            
        return from_node
    
    def _extract_where_part(self, statement) -> Optional[ASTNode]:
        where_tokens = []
        in_where = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() == 'WHERE':
                in_where = True
                continue
            elif token.ttype is tokens.Keyword and token.value.upper() in ['GROUP', 'HAVING', 'ORDER', 'LIMIT']:
                break
            elif in_where and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                where_tokens.append(token)
        
        if not where_tokens:
            return None
            
        where_node = ASTNode(
            node_type=ASTNodeType.WHERE,
            value="WHERE",
            metadata={"clause_type": "WHERE"}
        )
        
        conditions = self._parse_conditions(where_tokens)
        for condition in conditions:
            where_node.children.append(condition)
            
        return where_node
    
    def _extract_group_by_part(self, statement) -> Optional[ASTNode]:
        group_by_tokens = []
        in_group_by = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() in ['GROUP', 'BY']:
                if token.value.upper() == 'GROUP':
                    in_group_by = True
                continue
            elif token.ttype is tokens.Keyword and token.value.upper() in ['HAVING', 'ORDER', 'LIMIT']:
                break
            elif in_group_by and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                group_by_tokens.append(token)
        
        if not group_by_tokens:
            return None
            
        group_by_node = ASTNode(
            node_type=ASTNodeType.GROUP_BY,
            value="GROUP BY",
            metadata={"clause_type": "GROUP_BY"}
        )
        
        columns = self._parse_columns(group_by_tokens)
        for column in columns:
            group_by_node.children.append(column)
            
        return group_by_node
    
    def _extract_having_part(self, statement) -> Optional[ASTNode]:
        having_tokens = []
        in_having = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() == 'HAVING':
                in_having = True
                continue
            elif token.ttype is tokens.Keyword and token.value.upper() in ['ORDER', 'LIMIT']:
                break
            elif in_having and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                having_tokens.append(token)
        
        if not having_tokens:
            return None
            
        having_node = ASTNode(
            node_type=ASTNodeType.HAVING,
            value="HAVING",
            metadata={"clause_type": "HAVING"}
        )
        
        conditions = self._parse_conditions(having_tokens)
        for condition in conditions:
            having_node.children.append(condition)
            
        return having_node
    
    def _extract_order_by_part(self, statement) -> Optional[ASTNode]:
        order_by_tokens = []
        in_order_by = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() in ['ORDER', 'BY']:
                if token.value.upper() == 'ORDER':
                    in_order_by = True
                continue
            elif token.ttype is tokens.Keyword and token.value.upper() == 'LIMIT':
                break
            elif in_order_by and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                order_by_tokens.append(token)
        
        if not order_by_tokens:
            return None
            
        order_by_node = ASTNode(
            node_type=ASTNodeType.ORDER_BY,
            value="ORDER BY",
            metadata={"clause_type": "ORDER_BY"}
        )
        
        columns = self._parse_columns(order_by_tokens)
        for column in columns:
            order_by_node.children.append(column)
            
        return order_by_node
    
    def _extract_limit_part(self, statement) -> Optional[ASTNode]:
        limit_tokens = []
        in_limit = False
        
        for token in statement.flatten():
            if token.ttype is tokens.Keyword and token.value.upper() == 'LIMIT':
                in_limit = True
                continue
            elif in_limit and token.ttype not in [tokens.Whitespace, tokens.Newline]:
                limit_tokens.append(token)
        
        if not limit_tokens:
            return None
            
        limit_node = ASTNode(
            node_type=ASTNodeType.LIMIT,
            value="LIMIT",
            metadata={"clause_type": "LIMIT"}
        )
        
        if limit_tokens:
            limit_value = ''.join([str(token) for token in limit_tokens])
            limit_node.children.append(ASTNode(
                node_type=ASTNodeType.LITERAL,
                value=limit_value,
                metadata={"literal_type": "number"}
            ))
            
        return limit_node
    
    def _parse_columns(self, tokens_list) -> List[ASTNode]:
        columns = []
        current_column = []
        
        for token in tokens_list:
            if token.ttype is tokens.Punctuation and token.value == ',':
                if current_column:
                    column_node = self._create_column_node(current_column)
                    columns.append(column_node)
                    current_column = []
            else:
                current_column.append(token)
        
        if current_column:
            column_node = self._create_column_node(current_column)
            columns.append(column_node)
            
        return columns
    
    def _parse_tables(self, tokens_list) -> List[ASTNode]:
        tables = []
        current_table = []
        
        for token in tokens_list:
            if token.ttype is tokens.Punctuation and token.value == ',':
                if current_table:
                    table_node = self._create_table_node(current_table)
                    tables.append(table_node)
                    current_table = []
            else:
                current_table.append(token)
        
        if current_table:
            table_node = self._create_table_node(current_table)
            tables.append(table_node)
            
        return tables
    
    def _parse_conditions(self, tokens_list) -> List[ASTNode]:
        conditions = []
        current_condition = []
        
        for token in tokens_list:
            if token.ttype is tokens.Keyword and token.value.upper() in ['AND', 'OR']:
                if current_condition:
                    condition_node = self._create_condition_node(current_condition)
                    conditions.append(condition_node)
                    current_condition = []
                
                operator_node = ASTNode(
                    node_type=ASTNodeType.OPERATOR,
                    value=token.value.upper(),
                    metadata={"operator_type": "logical"}
                )
                conditions.append(operator_node)
            else:
                current_condition.append(token)
        
        if current_condition:
            condition_node = self._create_condition_node(current_condition)
            conditions.append(condition_node)
            
        return conditions
    
    def _create_column_node(self, tokens_list) -> ASTNode:
        column_text = ''.join([str(token) for token in tokens_list]).strip()
        
        if '(' in column_text and ')' in column_text:
            return ASTNode(
                node_type=ASTNodeType.FUNCTION,
                value=column_text,
                metadata={"function_name": column_text.split('(')[0].strip()}
            )
        else:
            return ASTNode(
                node_type=ASTNodeType.COLUMN,
                value=column_text,
                metadata={"column_name": column_text}
            )
    
    def _create_table_node(self, tokens_list) -> ASTNode:
        table_text = ''.join([str(token) for token in tokens_list]).strip()
        
        return ASTNode(
            node_type=ASTNodeType.TABLE,
            value=table_text,
            metadata={"table_name": table_text}
        )
    
    def _create_condition_node(self, tokens_list) -> ASTNode:
        condition_text = ''.join([str(token) for token in tokens_list]).strip()
        
        return ASTNode(
            node_type=ASTNodeType.CONDITION,
            value=condition_text,
            metadata={"condition_text": condition_text}
        )
    
    def ast_to_dict(self, ast_node: ASTNode) -> Dict[str, Any]:
        return {
            "node_type": ast_node.node_type,
            "value": ast_node.value,
            "metadata": ast_node.metadata,
            "children": [self.ast_to_dict(child) for child in ast_node.children]
        }
    
    def ast_to_json(self, ast_node: ASTNode) -> str:
        import json
        return json.dumps(self.ast_to_dict(ast_node), indent=2)