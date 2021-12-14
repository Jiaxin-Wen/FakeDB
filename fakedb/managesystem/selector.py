from enum import Enum, auto
import numpy as np


class SelectorKind(Enum):
    All = auto()
    Field = auto()
    Aggregation = auto()
    Counter = auto()
    
    
class Selector:
    def __init__(self, selector_kind, table_name, col_name, aggregation=None):
        self.kind = selector_kind
        self.table_name = table_name
        self.col_name = col_name
        self.aggregation = aggregation
        
    def __str__(self):
        return f"[Selector] table: {self.table_name}, col: {self.col_name}"
    
    def __call__(self, data):
        '''聚集函数'''
        if self.kind == SelectorKind.Counter: # Count *
            return len(data)
        elif self.kind == SelectorKind.Aggregation:
            if self.aggregation == 'Min':
                return min(data)
            elif self.aggregation == 'Max':
                return max(data)
            elif self.aggregation == 'Average':
                return np.mean(data)
            elif self.aggregation == 'Sum':
                return sum(data)
            elif self.aggregation == 'Count':
                return len(data)
            else:
                raise Exception(f'Unknown aggregation: {self.aggregation}')
            