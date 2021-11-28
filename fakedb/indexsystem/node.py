import numpy as np
from ..config import PAGE_SIZE
from .index_handler import IndexHandler

class TreeNode:
    def __init__(self, nodeType, page_id, parent_id, prev_id, next_id, key_values, handler):
        # nodeType: 'leaf' or 'inter'
        self.key_values = []
        self.nodeType = nodeType
        self.page_id = page_id
        self.parent_id = parent_id
        self.prev_id = prev_id
        self.next_id = next_id
        self.key_values = key_values
        self.handler = handler

    def isleaf(self):
        return self.nodeType == 'leaf'

    def lower_bound(self, key):
        # 返回第一个>=key的位置
        if not self.key_values:
            return None

        l, r = 0, len(self.key_values) - 1

        while l < r:
            mid = (l + r) >> 1
            if self.key_values[mid][0] < key:
                l = mid + 1
            else:
                r = mid

        if self.key_values[l][0] < key:
            return l + 1
        else:
            return l

    def upper_bound(self, key):
        # 返回第一个>key的位置
        if not self.key_values:
            return None

        l, r = 0, len(self.key_values) - 1

        while l < r:
            mid = (l + r) >> 1
            if self.key_values[mid][0] <= key:
                l = mid + 1
            else:
                r = mid

        if self.key_values[l][0] <= key:
            return l + 1
        else:
            return l

    def split(self):
        mid = len(self.key_values) // 2
        right_key_values = self.key_values[mid:]
        new_mid_val = self.key_values[mid-1][0]
        self.key_values = self.key_values[:mid]
        return right_key_values, new_mid_val

    def current_page_size(self):
        if self.isleaf():
            return 64 + 24 * len(self.key_values)

        else:
            return 40 + 16 * len(self.key_values)

    def insert(self, key, val):
        if self.isleaf():
            pos = self.upper_bound(key)
            if pos is None:
                pos = 0

            self.key_values.insert(pos, [key, val])

        else:
            pos = self.lower_bound(key)
            if pos is None:
                page_id = self.handler.new_page()
                node = TreeNode(nodeType='leaf', page_id=page_id, parent_id=self.page_id, prev_id=0, next_id=0, key_values=[], handler=self.handler)
                self.key_values.append([key, node])
                node.insert(key, val)

            else:
                if pos >= len(self.key_values):
                    pos -= 1
                
                node = self.key_values[pos][1]
                node.insert(key, val)
                if self.key_values[pos][0] < key:
                    self.key_values[pos][0] = key
                
                # check split
                if node.current_page_size() > PAGE_SIZE:
                    right_key_values, left_max_key = node.split()
                    old_key = self.key_values[pos][0]
                    self.key_values[pos][0] = left_max_key

                    page_id = self.handler.new_page()
                    if node.isleaf():
                        # TODO: 这两行的顺序应该不能调换？
                        newnode = TreeNode(nodeType='leaf', page_id=page_id, parent_id=self.page_id, prev_id=node.page_id, next_id=node.next_id, key_values=right_key_values, handler=self.handler)
                        node.next_id = page_id
                    else:
                        newnode = TreeNode(nodeType='inter', page_id=page_id, parent_id=self.page_id, prev_id=None, next_id=None, key_values=right_key_values, handler=self.handler)

                    self.key_values.insert(pos+1, [old_key, newnode])


    def remove(self, key, val):
        # 如果成功删除，则返回删除之后的最大key(如果没有就返回None)，否则返回None

        if self.isleaf():
            pos = self.lower_bound(key)
            if pos is None or pos >= len(self.key_values):
                return None
            if self.key_values[pos][0] != key or self.key_values[pos][1] != val:
                return None

            self.key_values.pop(pos)
            return self.key_values[-1][0]

        else:
            pos = self.lower_bound(key)
            if pos is None or pos >= len(self.key_values):
                return None

            node = self.key_values[pos][1]
            new_max_key = node.remove(key, val)


            # check merge
            # FIXME: 叶子节点的next_id, prev_id需要处理吗?
            if len(node.key_values) == 0:
                self.key_values.pop(pos)


            if new_max_key is not None:
                self.key_values[pos][0] = new_max_key      
                if len(self.key_values) > 0:
                    return self.key_values[-1][0]
                else:
                    # 删空了
                    return None

            else:
                # 删除失败或者没有新的最大值
                return None

    def search(self, key):
        if self.isleaf():
            pos = self.lower_bound(key)
            if pos is None or pos >= len(self.key_values):
                return None

            if self.key_values[pos][0] == key:
                return self.key_values[pos][1]

            return None

        else:
            pos = self.lower_bound(key)
            if pos is None or pos >= len(self.key_values):
                return None

            return self.key_values[pos][1].search(key)

    def rangeSearch(self, l, r):
        # 返回key在[l,r]之间的所有value
        if self.isleaf():
            lpos = self.lower_bound(l)
            if lpos is None or lpos >= len(self.key_values):
                return None

            rpos = self.upper_bound(r)
            return [self.key_values[i][1] for i in range(lpos, rpos)]

        else:
            lpos = self.lower_bound(l)
            if lpos is None or lpos >= len(self.key_values):
                return None
            
            rpos = self.upper_bound(r)
            res = []
            for i in range(lpos, rpos):
                ret = self.key_values[i][1].rangeSearch(l, r)
                if ret is not None:
                    res.extend(ret)

            return res

    def toArray(self):
        if self.isleaf():
            a = np.zeros(PAGE_SIZE//8, np.int64)
            a[0:5] = [1, self.parent_id, self.prev_id,
                      self.next_id, len(self.key_values)]
            for i, (key, rid) in enumerate(self.key_values):
                a[3*i+5:3*i+8] = [key, rid.page_id, rid.slot_id]

            a.dtype = np.uint8
            return a

        else:
            a = np.zeros(PAGE_SIZE//8, np.int64)
            a[0:3] = [0, self.parent_id, len(self.key_values)]
            for i, (key, node) in enumerate(self.key_values):
                a[2*i+3:2*i+5] = [key, node.page_id]

            a.dtype=np.uint8
            return a
