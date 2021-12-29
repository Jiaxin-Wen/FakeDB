from enum import Enum, auto

class ConditionKind(Enum):
    Compare = auto()
    IsNull = auto()
    Like = auto()
    In = auto()


class Condition:
    def __init__(self, condition_kind, table_name, col_name, operator=None, value=None, table_name2=None, col_name2=None, value_null_true=False, col_null_true=False):
        self.kind = condition_kind
        self.table_name = table_name
        self.col_name = col_name
        self.operator = operator
        self.value = value
        self.table_name2 = table_name2
        self.col_name2 = col_name2
        # for null foreign key
        self.value_null_true = value_null_true
        self.col_null_true = col_null_true

    def __str__(self):
        return f"[Condition] table: {self.table_name}, col: {self.col_name}, operator = {self.operator}, value = {self.value}, table2 = {self.table_name2}, cols = {self.col_name2}"