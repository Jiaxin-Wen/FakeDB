from enum import Enum, auto


class SelectorKind(Enum):
    All = auto()
    Field = auto()
    Aggregation = auto()
    Counter = auto()
    
    
class Selector:
    def __init__(self, selector_kind, table_name, col_name):
        self.kind = selector_kind
        self.table_name = table_name
        self.col_name = col_name
        
    def __str__(self):
        return f"[Selector] table: {self.table_name}, col: {self.col_name}"