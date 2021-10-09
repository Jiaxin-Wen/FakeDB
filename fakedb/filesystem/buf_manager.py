import os

from ..config import CACHE_SIZE


class BufManager:
    def __init__(self):
        pass


    def close(self, fd):
        '''
        TODO:
        关闭一个文件, 更新cache
        fd: file descsriptor
        '''
        pass


    def write(self, fd, pd, data):
        '''
        TODO:
        写文件, 写到cache中
        '''
        pass

    def shutdown(self):
        '''
        TODO:
        退出
        '''
        pass