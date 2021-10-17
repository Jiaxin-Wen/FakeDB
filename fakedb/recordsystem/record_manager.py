


from ..filesystem.file_manager import FileManager
from .utils import get_record_capacity, get_bitmap_len
from .header import Header
from .filehandle import FileHandle

class RecordManager:

    def __init__(self, file_manager):
        self.file_manager = file_manager

    def create_file(self, filename, record_len):
        '''
        创建一张表
        filename: 待创建的文件名(数据库名+表名)
        record_len: 一条记录的长度
        '''
        # 创建文件
        self.file_manager.create_file(filename)

        # 添加头页
        fd = self.file_manager.open_file(filename) # 打开文件
        record_capacity = get_record_capacity(record_len) # 计算每页可存储记录的最大条数
        bitmap_len = get_bitmap_len(record_capacity)
        header = Header(
            record_len = record_len, 
            record_capacity = record_capacity, 
            record_num = 0,
            page_num = 1,
            filename = filename,
            bitmap_len = bitmap_len)
        header_data = header.serialize()
        self.file_manager.write_page(fd, 0, header_data) # 写在第一页

        # 关闭文件
        self.file_manager.close_file(filename)

    def remove_file(self, filename):
        '''删除文件'''
        self.file_manager.remove_file(filename)

    def open_file(self, filename):
        '''
        TODO:
        打开文件, 获得其句柄
        '''
        pass

    def close_file(self, filename):
        '''
        TODO:
        关闭文件
        '''
        fd = None
        self.file_manager.close_file(fd)
        