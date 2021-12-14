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
        if self.kind == SelectorKind.Counter:
            name = 'Count *'
        elif self.kind == SelectorKind.All:
            name = 'Select *'
        elif self.kind ==  SelectorKind.Field:
            name = 'Select '
        else:
            name = self.aggregation
        return f"{name} -> {self.table_name}.{self.col_name}"
    
    def __call__(self, data):
        '''聚集函数'''
        if self.kind == SelectorKind.Field:
            return data
        elif self.kind == SelectorKind.Counter: # Count *
            return len(data)
        elif self.kind == SelectorKind.Aggregation:
            if self.aggregation == 'MIN':
                return min(data)
            elif self.aggregation == 'MAX':
                return max(data)
            elif self.aggregation == 'AVG':
                return np.mean(data)
            elif self.aggregation == 'SUM':
                return sum(data)
            elif self.aggregation == 'COUNT':
                return len([i for i in data if i is not None])
            else:
                raise Exception(f'Unknown aggregation: {self.aggregation}')
            