'''
每张表建立一个索引(FileIndex)
管理所有索引
'''

from .fileindex import FileIndex
from .index_handler import IndexHandler


class IndexManager:
    def __init__(self, file_manager):
        self.file_manager = file_manager
        
        self.file2handler = {} # filename -> indexhandler
        # TODO: 应该把index包装在indexhandler里, 不要维护两个。。。
        pass
    
    def get_handler(self, filename):
        '''
        filename: 数据库名 + 表名
        获取一张表的索引
        '''
        if filename not in self.file2handler:
            handler = IndexHandler(filename, self.file_manager)
            self.file2handler[filename] = handler
        
        return self.file2handler[filename]
    
    def close_handler(self, filename):
        '''
        filename: 数据库名 + 表名
        关闭一张表的索引
        '''
        if filename not in self.file2handler:
            return False
        else:
            # TODO: 目前只关闭了handler, 没有关闭index
            handler = self.file2handler.pop(filename)
            handler.close()
            return True
        
    def shutdown(self):
        for filename in self.file2handler:
            self.close_handler(filename)