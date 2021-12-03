import numpy as np

from ..config import INDEX_SUFFIX
from ..filesystem import FileManager


class IndexHandler:
    '''
    负责索引和文件系统的交互
    '''
    def __init__(self, filename, file_manager: FileManager):
        '''
        filename: 数据库名 + 表名
        '''
        self.file_manager = file_manager
        index_filename = f'{filename}/{INDEX_SUFFIX}'
        if not self.file_manager.exists(index_filename):
            self.file_manager.create_file(index_filename)
        self.fd = self.file_manager.open_file(index_filename)
    
    def read_page(self, page_id):
        return self.file_manager.read_page(self.fd, page_id)
    
    def write_page(self, page_id, data):
        return self.file_manager.write_page(self.fd, page_id, data)
    
    def new_page(self):
        return self.file_manager.new_page(self.fd)
    
    def close(self):
        self.file_manager.close_file(self.fd)