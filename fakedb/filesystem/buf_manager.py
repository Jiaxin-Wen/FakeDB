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

    def _write(self, fd, pd, idx):
        '''
        写到存储中 不直接调用
        '''
        os.lseek(fd, pd << PAGE_SIZE_BITS, os.SEEK_SET)
        os.write(fd, self.pages[idx].tobytes())
        
    def _read(self, fd, pd):
        os.lseek(fd, pd << PAGE_SIZE_BITS, 0) # 设置偏移量
        return os.read(fd, PAGE_SIZE) # 读一页数据, 返回

    def write(self, fd, pd, data):
        '''
        写文件, 写到cache中
        '''
        if fd not in self.fdpd_to_idx:
            self.fdpd_to_idx[fd] = {}
        if pd not in self.fdpd_to_idx[fd]:
            idx, need_write_back = self.lru.assign()
            if need_write_back:
                self._write(fd, pd, idx)
            self.fdpd_to_idx[fd][pd] = idx
        idx = self.fdpd_to_idx[fd][pd]
        self.lru.access(idx)
        self.pages[idx] = data

    def read(self, fd, pd):
        '''
        读
        - 读cache
        - 不在cache中的话就读存储, 放到cache
        '''
        try: # 读cache
            idx = self.fdpd_to_idx[fd][pd]
            data = self.pages[idx]
        except: # 读存储 放回cache
            idx, need_write_back = self.lru.assign()
            if need_write_back:
                self._write(fd, pd, idx)
                if fd not in self.fdpd_to_idx:
                    self.fdpd_to_idx[fd] = {}
                self.fdpd_to_idx[fd][pd] = idx
                
            data = self._read(fd, pd)
            self.pages[idx] = data
            
        self.lru.access(idx)
        return data