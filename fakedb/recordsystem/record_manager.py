


from ..filesystem.file_manager import FileManager
from .utils import get_record_size


class RecordManager:

    def __init__(self, file_manager):
        self.file_manager = file_manager

    def create_file(self, filename, record_len):
        '''
        TODO:
        filename: 待创建的文件名(数据库名+表名)
        record_len: 一条记录的长度
        '''
        # 创建文件
        self.file_manager.create_file(filename)

        # 添加头页
        fd = self.file_manager.open_file(filename) # 打开文件
        record_size = get_record_size(record_len) # 计算记录最大条数


        # 关闭文件
        self.file_manager.close_file(filename)

    def remove_file(self, filename):
        '''删除文件'''
        self.file_manager.remove_file(filename)

    def open_file(self, filename):
        '''
        TODO:
        打开文件
        '''
        pass