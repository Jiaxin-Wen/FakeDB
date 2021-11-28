'''
基于B+树的索引, 对外提供查询接口
'''


import numpy as np

from .index_handler import IndexHandler
from .node import TreeNode
from ..recordsystem.rid import RID
from ..config import PAGE_SIZE


class FileIndex:
    def __init__(self, handler, root_id):
        self.root_id = root_id # 根节点的页号
        self.handler = handler
        data = self.handler.read_page(root_id)
        data.dtype = np.int64
        nodeType = data[0]
        parent_id = data[1]
        assert nodeType == 0
        assert parent_id == root_id
        self.rootNode = self.get_node(root_id)    
    
    def get_node(self, page_id):
        data = self.handler.read_page(page_id)
        data.dtype = np.int64
        parent_id = data[1]
        if data[0] == 1:
            # leaf node
            prev_id = data[2]
            next_id = data[3]
            length = data[4]
            key_values = [[data[3*i+5], RID(data[3*i+6],data[3*i+7])] for i in range(length)]
            return TreeNode(nodeType='leaf', page_id=page_id, parent_id=parent_id, prev_id=prev_id, next_id=next_id, key_values=key_values, handler=self.handler)

        else:
            # inter node
            length = data[2]
            key_values = [[data[2*i+3], data[2*i+4]] for i in range(length)]
            return TreeNode(nodeType='inter', page_id=page_id, parent_id=parent_id, prev_id=None, next_id=None, key_values=key_values, handler=self.handler)
    
    def writeback(self):
        q = [self.rootNode]
        while q:
            node = q.pop(0)
            page_id = node.page_id
            if not node.isleaf():
                for k, v in node.key_values:
                    q.append(v)

            self.handler.write_page(page_id, node.toArray())

    def insert(self, key, val):
        self.rootNode.insert(key, val)

        # check split
        if self.rootNode.current_page_size() > PAGE_SIZE:
            new_root_id = self.handler.new_page()
            newroot = TreeNode(nodeType='inter', page_id=new_root_id, parent_id=new_root_id, prev_id=None, next_id=None, key_values=[], handler=self.handler)
            self.rootNode.parent_id = new_root_id
            max_key = self.rootNode.key_values[-1][0]
            right_key_values, left_max_key = self.rootNode.split()

            right_page_id = self.handler.new_page()
            right_node = TreeNode(nodeType='inter', page_id=right_page_id, parent_id=new_root_id, prev_id=None, next_id=None, key_values=right_key_values, handler=self.handler)

            temp_node = self.rootNode
            self.rootNode = newroot
            self.root_id = new_root_id
            self.rootNode.key_values = [[left_max_key, temp_node], [max_key, right_node]]

    
    def remove(self, key, val):
        return self.rootNode.remove(key, val)

    def search(self, key):
        return self.rootNode.search(key)

    def rangeSearch(self, l, r):
        return self.rootNode.rangeSearch(l, r)
