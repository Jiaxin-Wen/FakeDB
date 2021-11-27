import numpy as np

from ..config import INDEX_FILE_SUFFIX
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
        index_filename = f'{filename}/{INDEX_FILE_SUFFIX}'
        if not self.file_manager.exists(index_filename):
            self.file_manager.create_file(index_filename)
        self.fd = self.file_manager.open_file(index_filename)
        
    # TODO: 封装filemanager的交互