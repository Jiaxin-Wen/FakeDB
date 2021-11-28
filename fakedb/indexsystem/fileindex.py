'''
基于B+树的索引, 对外提供查询接口
'''


import numpy as np

from .index_handler import IndexHandler
from .node import TreeNode

class FileIndex:
    def __init__(self, handler, root_id):
        self.root_id = root_id # 根节点的页号
        self.handler = handler
        self.root = TreeNode('inter', root_id, root_id, ) # TODO: 未完成
    
    
    