import os
import numpy as np
from ..config import CACHE_SIZE, PAGE_SIZE
from .lru import LRU

class BufManager:
    def __init__(self):
        self.pages = np.zeros((CACHE_SIZE, PAGE_SIZE), dtype=np.uint8)
        self.fdpd_to_idx = {}
        self.lru = LRU()

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
        if fd not in self.fdpd_to_idx:
            self.fdpd_to_idx[fd] = {}
        if pd not in self.fdpd_to_idx[fd]:
            pass
        pass