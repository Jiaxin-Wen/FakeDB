'''
FileManager with buffer

'''

import os


class FileManager:

    def __init__(self):
        self.fd2name = {} # 维护打开的文件名和fd的映射
        self.name2fd = {}

    def create_file(self, filename):
        '''创建文件'''
        os.mknod(filename)

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
        # TODO: 清空cache中该文件对应的页, 写回
        os.close(fd)
        self.name2fd.pop(self.fd2name.pop(fd));

    


    