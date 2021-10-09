import os
import numpy as np
from ..config import CACHE_SIZE, PAGE_SIZE, PAGE_SIZE_BITS
from .lru import LRU


class BufManager:
    def __init__(self):
        self.pages = np.zeros((CACHE_SIZE, PAGE_SIZE), dtype=np.uint8)
        self.fdpd_to_idx = {}
        self.lru = LRU(CACHE_SIZE)

    def close(self, fd):
        '''
        关闭一个文件, 更新cache
        fd: file descsriptor
        '''
        if fd in self.fdpd_to_idx:
            d = self.fdpd_to_idx[fd]
            for pd, idx in d.items():
                self.lru.free(idx)
                self.write_back(fd, pd, idx)
            self.fdpd_to_idx.pop(fd)

    def write_back(self, fd, pd, idx):
        os.lseek(fd, pd << PAGE_SIZE_BITS, os.SEEK_SET)
        os.write(fd, self.pages[idx].tobytes())

    def write(self, fd, pd, data):
        '''
        写文件, 写到cache中
        '''
        if fd not in self.fdpd_to_idx:
            self.fdpd_to_idx[fd] = {}
        if pd not in self.fdpd_to_idx[fd]:
            idx, need_write_back = self.lru.assign()
            if need_write_back:
                self.write_back(self, fd, pd, idx)
            self.fdpd_to_idx[fd][pd] = idx
        idx = self.fdpd_to_idx[fd][pd]
        self.lru.access(idx)
        self.pages[idx] = data

    def shutdown(self):
        '''
        TODO:
        退出
        '''
        for fd, d in self.fdpd_to_idx.items():
            for pd, idx in d.items():
                self.lru.free(idx)
                self.write_back(fd, pd, idx)
        self.fdpd_to_idx = {}
