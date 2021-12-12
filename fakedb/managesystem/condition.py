from enum import Enum, auto

class ConditionKind(Enum):
    Compare = auto()
    IsNull = auto()
    Like = auto()
    In = auto()


class Condition:
    def __init__(self, condition_kind, table_name, col_name, operator=None, value=None, table_name2=None, col_name2=None):
        self.kind = condition_kind
        self.table_name = table_name
        self.col_name = col_name
        self.operator = operator
        self.value = value
        self.table_name2 = table_name2
        self.col_name2 = col_name2
