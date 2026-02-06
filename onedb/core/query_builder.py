"""
Universal Query Builder for SQL and NoSQL databases.
Provides a fluent interface for building queries.
"""

from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
from copy import deepcopy


class Operator(Enum):
    """Query operators."""
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    BETWEEN = "BETWEEN"
    REGEX = "~"


class JoinType(Enum):
    """SQL join types."""
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"
    CROSS = "CROSS JOIN"


@dataclass
class Condition:
    """Single query condition."""
    column: str
    operator: Operator
    value: Any
    logic: str = "AND"  # AND, OR


@dataclass
class Join:
    """Join clause."""
    join_type: JoinType
    table: str
    on: str


class Query:
    """
    Fluent Query Builder.
    
    Supports both SQL and NoSQL query generation.
    
    Example:
        # SQL style
        query = (Query("users")
            .select("id", "name", "email")
            .where("age", ">", 18)
            .where("status", "=", "active")
            .order_by("created_at", "DESC")
            .limit(10))
        
        sql, params = query.to_sql()
        # SELECT id, name, email FROM users 
        # WHERE age > %s AND status = %s 
        # ORDER BY created_at DESC LIMIT 10
        
        # MongoDB style
        mongo_query = query.to_mongo()
        # {"age": {"$gt": 18}, "status": "active"}
    """
    
    def __init__(self, table: str):
        """
        Initialize query builder.
        
        Args:
            table: Table/collection name
        """
        self._table = table
        self._columns: List[str] = ["*"]
        self._conditions: List[Condition] = []
        self._joins: List[Join] = []
        self._group_by: List[str] = []
        self._having: List[Condition] = []
        self._order_by: List[Tuple[str, str]] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._distinct = False
        self._alias: Optional[str] = None
    
    def select(self, *columns: str) -> "Query":
        """
        Set columns to select.
        
        Args:
            *columns: Column names
            
        Returns:
            Self for chaining
        """
        self._columns = list(columns) if columns else ["*"]
        return self
    
    def distinct(self) -> "Query":
        """Add DISTINCT clause."""
        self._distinct = True
        return self
    
    def where(
        self, 
        column: str, 
        operator: Union[str, Operator] = Operator.EQ,
        value: Any = None
    ) -> "Query":
        """
        Add WHERE condition.
        
        Args:
            column: Column name
            operator: Comparison operator or value if using "="
            value: Comparison value
            
        Returns:
            Self for chaining
            
        Example:
            query.where("age", ">", 18)
            query.where("name", "John")  # Equals
            query.where("status", Operator.IN, ["active", "pending"])
        """
        if value is None and not isinstance(operator, Operator):
            # Short syntax: where("name", "John") -> where("name", "=", "John")
            value = operator
            operator = Operator.EQ
        
        if isinstance(operator, str):
            operator = Operator(operator)
        
        self._conditions.append(Condition(column, operator, value))
        return self
    
    def or_where(
        self, 
        column: str, 
        operator: Union[str, Operator] = Operator.EQ,
        value: Any = None
    ) -> "Query":
        """Add OR WHERE condition."""
        if value is None and not isinstance(operator, Operator):
            value = operator
            operator = Operator.EQ
        
        if isinstance(operator, str):
            operator = Operator(operator)
        
        self._conditions.append(Condition(column, operator, value, logic="OR"))
        return self
    
    def where_in(self, column: str, values: List[Any]) -> "Query":
        """Add WHERE IN condition."""
        return self.where(column, Operator.IN, values)
    
    def where_null(self, column: str) -> "Query":
        """Add WHERE IS NULL condition."""
        return self.where(column, Operator.IS_NULL, None)
    
    def where_not_null(self, column: str) -> "Query":
        """Add WHERE IS NOT NULL condition."""
        return self.where(column, Operator.IS_NOT_NULL, None)
    
    def where_between(
        self, 
        column: str, 
        start: Any, 
        end: Any
    ) -> "Query":
        """Add WHERE BETWEEN condition."""
        return self.where(column, Operator.BETWEEN, (start, end))
    
    def like(self, column: str, pattern: str) -> "Query":
        """Add LIKE condition."""
        return self.where(column, Operator.LIKE, pattern)
    
    def join(
        self, 
        table: str, 
        on: str, 
        join_type: JoinType = JoinType.INNER
    ) -> "Query":
        """
        Add JOIN clause.
        
        Args:
            table: Table to join
            on: Join condition
            join_type: Type of join
            
        Example:
            query.join("orders", "users.id = orders.user_id")
            query.join("profiles", "users.id = profiles.user_id", JoinType.LEFT)
        """
        self._joins.append(Join(join_type, table, on))
        return self
    
    def left_join(self, table: str, on: str) -> "Query":
        """Add LEFT JOIN."""
        return self.join(table, on, JoinType.LEFT)
    
    def right_join(self, table: str, on: str) -> "Query":
        """Add RIGHT JOIN."""
        return self.join(table, on, JoinType.RIGHT)
    
    def group_by(self, *columns: str) -> "Query":
        """Add GROUP BY clause."""
        self._group_by.extend(columns)
        return self
    
    def having(
        self, 
        column: str, 
        operator: Union[str, Operator],
        value: Any
    ) -> "Query":
        """Add HAVING clause."""
        if isinstance(operator, str):
            operator = Operator(operator)
        self._having.append(Condition(column, operator, value))
        return self
    
    def order_by(self, column: str, direction: str = "ASC") -> "Query":
        """
        Add ORDER BY clause.
        
        Args:
            column: Column to order by
            direction: ASC or DESC
        """
        self._order_by.append((column, direction.upper()))
        return self
    
    def limit(self, count: int) -> "Query":
        """Set LIMIT."""
        self._limit = count
        return self
    
    def offset(self, count: int) -> "Query":
        """Set OFFSET."""
        self._offset = count
        return self
    
    def paginate(self, page: int, per_page: int = 10) -> "Query":
        """
        Add pagination.
        
        Args:
            page: Page number (1-indexed)
            per_page: Items per page
        """
        self._limit = per_page
        self._offset = (page - 1) * per_page
        return self
    
    def alias(self, name: str) -> "Query":
        """Set table alias."""
        self._alias = name
        return self
    
    # ==================== Output Methods ====================
    
    def to_sql(self, placeholder: str = "%s") -> Tuple[str, List[Any]]:
        """
        Generate SQL query.
        
        Args:
            placeholder: Parameter placeholder (%s, ?, :1, etc.)
            
        Returns:
            Tuple of (sql_string, parameters)
        """
        params = []
        sql_parts = []
        
        # SELECT
        distinct = "DISTINCT " if self._distinct else ""
        columns = ", ".join(self._columns)
        sql_parts.append(f"SELECT {distinct}{columns}")
        
        # FROM
        table = self._table
        if self._alias:
            table = f"{table} AS {self._alias}"
        sql_parts.append(f"FROM {table}")
        
        # JOINs
        for join in self._joins:
            sql_parts.append(f"{join.join_type.value} {join.table} ON {join.on}")
        
        # WHERE
        if self._conditions:
            where_clauses = []
            for i, cond in enumerate(self._conditions):
                logic = "" if i == 0 else f" {cond.logic} "
                
                if cond.operator == Operator.IS_NULL:
                    where_clauses.append(f"{logic}{cond.column} IS NULL")
                elif cond.operator == Operator.IS_NOT_NULL:
                    where_clauses.append(f"{logic}{cond.column} IS NOT NULL")
                elif cond.operator == Operator.IN:
                    placeholders = ", ".join([placeholder] * len(cond.value))
                    where_clauses.append(
                        f"{logic}{cond.column} IN ({placeholders})"
                    )
                    params.extend(cond.value)
                elif cond.operator == Operator.BETWEEN:
                    where_clauses.append(
                        f"{logic}{cond.column} BETWEEN {placeholder} AND {placeholder}"
                    )
                    params.extend(cond.value)
                else:
                    where_clauses.append(
                        f"{logic}{cond.column} {cond.operator.value} {placeholder}"
                    )
                    params.append(cond.value)
            
            sql_parts.append("WHERE " + "".join(where_clauses))
        
        # GROUP BY
        if self._group_by:
            sql_parts.append(f"GROUP BY {', '.join(self._group_by)}")
        
        # HAVING
        if self._having:
            having_clauses = []
            for cond in self._having:
                having_clauses.append(
                    f"{cond.column} {cond.operator.value} {placeholder}"
                )
                params.append(cond.value)
            sql_parts.append("HAVING " + " AND ".join(having_clauses))
        
        # ORDER BY
        if self._order_by:
            order_parts = [f"{col} {dir}" for col, dir in self._order_by]
            sql_parts.append(f"ORDER BY {', '.join(order_parts)}")
        
        # LIMIT
        if self._limit is not None:
            sql_parts.append(f"LIMIT {self._limit}")
        
        # OFFSET
        if self._offset is not None:
            sql_parts.append(f"OFFSET {self._offset}")
        
        return " ".join(sql_parts), params
    
    def to_mongo(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Generate MongoDB query.
        
        Returns:
            Tuple of (filter_dict, options_dict)
        """
        mongo_filter = {}
        options = {}
        
        # Convert conditions to MongoDB format
        operator_map = {
            Operator.EQ: "$eq",
            Operator.NE: "$ne",
            Operator.GT: "$gt",
            Operator.GTE: "$gte",
            Operator.LT: "$lt",
            Operator.LTE: "$lte",
            Operator.IN: "$in",
            Operator.NOT_IN: "$nin",
            Operator.REGEX: "$regex",
        }
        
        for cond in self._conditions:
            if cond.operator == Operator.EQ:
                mongo_filter[cond.column] = cond.value
            elif cond.operator in operator_map:
                mongo_filter[cond.column] = {
                    operator_map[cond.operator]: cond.value
                }
            elif cond.operator == Operator.IS_NULL:
                mongo_filter[cond.column] = None
            elif cond.operator == Operator.IS_NOT_NULL:
                mongo_filter[cond.column] = {"$ne": None}
            elif cond.operator == Operator.BETWEEN:
                mongo_filter[cond.column] = {
                    "$gte": cond.value[0],
                    "$lte": cond.value[1]
                }
            elif cond.operator == Operator.LIKE:
                # Convert SQL LIKE to regex
                pattern = cond.value.replace("%", ".*").replace("_", ".")
                mongo_filter[cond.column] = {"$regex": pattern, "$options": "i"}
        
        # Projection
        if self._columns != ["*"]:
            options["projection"] = {col: 1 for col in self._columns}
        
        # Sort
        if self._order_by:
            options["sort"] = [
                (col, 1 if dir == "ASC" else -1) 
                for col, dir in self._order_by
            ]
        
        # Limit & Skip
        if self._limit:
            options["limit"] = self._limit
        if self._offset:
            options["skip"] = self._offset
        
        return mongo_filter, options
    
    def copy(self) -> "Query":
        """Create a copy of this query."""
        return deepcopy(self)
    
    def __str__(self) -> str:
        sql, _ = self.to_sql()
        return sql


class QueryBuilder:
    """
    Static query builder helper.
    
    Example:
        QueryBuilder.select("users").where("active", True).to_sql()
    """
    
    @staticmethod
    def select(table: str) -> Query:
        """Start SELECT query."""
        return Query(table)
    
    @staticmethod
    def table(table: str) -> Query:
        """Alias for select."""
        return Query(table)
    
    @staticmethod
    def raw(sql: str, params: Optional[List[Any]] = None) -> Tuple[str, List[Any]]:
        """Create raw query."""
        return sql, params or []
