import os

from .buf_manager import BufManager
from ..config import PAGE_SIZE, PAGE_SIZE_BITS



class FileManager:

    def __init__(self):

        # 维护打开的文件名和fd的映射
        self.fd2name = {} 
        self.name2fd = {}

        # buffer manager
        self.buf_manager = BufManager()

    def create_file(self, filename):
        '''创建文件'''
        open(filename, 'a').close()

    def remove_file(self, filename):
        '''删除文件'''
        os.remove(filename)

    def open_file(self, filename):
        '''打开文件
        返回file descriptor(int)
        '''
        if filename in self.name2id:
            raise Exception(f"file {filename} has been openned")
        fd = os.open(filename)
        self.fd2name[fd] = filename
        self.name2fd[filename] = fd
        return fd
        

    def close_file(self, fd):
        '''关闭文件'''
        self.buf_manager.close(fd) # 清空cache中该文件对应的页
        os.close(fd)
        self.name2fd.pop(self.fd2name.pop(fd));
    
    def read_page(self, fd, pd):
        '''读取一页数据
        fd: file id, 文件描述符
        pd: page id, 页号

        return: 读出的数据
        '''
        os.lseek(fd, pd << PAGE_SIZE_BITS, 0) # 设置偏移量
        return os.read(fd, PAGE_SIZE) # 读一页数据, 返回

    def write_page(self, fd, pd, data):
        '''写回一页数据
        fd: file id, 文件描述符
        pd: page id, 页号
        data: 要写入的数据
        return: 无返回值
        '''
        self.buf_manager.write(fd, pd, data)

    def new_page(self, fd, data):
        pos = os.lseek(fd, 0, os.SEEK_END)
        os.write(fd, data.tobytes())
        return pos >> PAGE_SIZE_BITS

    def shutdown(self):
        '''
        退出
        '''
        for fd in self.fd2name:
            self.close_file(fd)  

        assert not any(self.fd2name)
        assert not any(self.name2fd)

    def get_fd_by_name(self, name):
        return self.name2fd[name]