'''
每张表建立一个索引(FileIndex)
管理所有索引
'''

from .fileindex import FileIndex
from .index_handler import IndexHandler


class IndexManager:
    def __init__(self, file_manager):
        self.file_manager = file_manager
        
        self.file2index = {} # file -> fileindex

    def create_index(self, filename):
        '''
        filename: 数据库名 + 表名 + 列名
        '''
        handler = IndexHandler(filename, self.file_manager)
        index = FileIndex(handler, handler.new_page())
        index.writeback()
        self.file2index[filename] = index
        return index

    def open_index(self, filename, root_id):
        '''
        filename: 数据库名 + 表名 + 列名
        root_id: 根节点页号
        '''
        handler = IndexHandler(filename, self.file_manager)
        index = FileIndex(handler, root_id)
        # load放到init中了
        self.file2index[filename] = index
        return index

    def close_index(self, filename):
        if filename not in self.file2index:
            return None
        else:
            index = self.file2index.pop(filename)
            index.write_back()
            
        
    def shutdown(self):
        for filename in self.file2index:
            self.close_index(filename)