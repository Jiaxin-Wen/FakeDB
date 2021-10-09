import os

from ..config import CACHE_SIZE


class BufManager:
    def __init__(self):
        pass


    def close(self, fd):
        '''
        关闭一个文件, 更新cache
        fd: file descsriptor
        '''
        pass


    def write(self, fd, pd, data):
        '''
        写文件, 写到cache中
        '''
        pass