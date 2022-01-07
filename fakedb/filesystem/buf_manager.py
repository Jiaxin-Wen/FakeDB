import os
import numpy as np
from ..config import CACHE_SIZE, PAGE_SIZE, PAGE_SIZE_BITS
from .lru import LRU


class BufManager:
    def __init__(self):
        self.pages = np.zeros((CACHE_SIZE, PAGE_SIZE), dtype=np.uint8)
        self.fdpd_to_idx = {}
        self.idx_to_fdpd = {}
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
                self._write(fd, pd, idx)
                self.idx_to_fdpd.pop(idx)
            self.fdpd_to_idx.pop(fd)

    def _write(self, fd, pd, idx):
        '''
        写到存储中 不直接调用
        '''
        # if pd == 0:
        #     print(fd, self.pages[idx])
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
                print(f'in write need write back, fd:{fd}, pd:{pd}, idx:{idx}, _fd and _pd:{self.idx_to_fdpd[idx]}')
                _fd, _pd = self.idx_to_fdpd[idx]
                self._write(_fd, _pd, idx)
                self.fdpd_to_idx[_fd].pop(_pd)
                if not self.fdpd_to_idx[_fd]:
                    self.fdpd_to_idx.pop(_fd)

            self.fdpd_to_idx[fd][pd] = idx
        idx = self.fdpd_to_idx[fd][pd]
        self.lru.access(idx)
        self.pages[idx] = data
        self.idx_to_fdpd[idx] = (fd, pd)

    def read(self, fd, pd):
        '''
        读
        - 读cache
        - 不在cache中的话就读存储, 放到cache
        '''
        try: # 读cache
            idx = self.fdpd_to_idx[fd][pd]
            data = self.pages[idx]
            # print(f'read from cache idx:{idx}')
        except: # 读存储 放回cache
            idx, need_write_back = self.lru.assign()
            # print(f'assign cache idx:{idx}')
            # print(f'pd:{pd}, idx:{idx}')
            # if need_write_back is True:
            #     print(f'pd:{pd}, idx:{idx}')
            # assert need_write_back is False
            if need_write_back:
                _fd, _pd = self.idx_to_fdpd[idx]
                self._write(_fd, _pd, idx)
                self.fdpd_to_idx[_fd].pop(_pd)
                if not self.fdpd_to_idx[_fd]:
                    self.fdpd_to_idx.pop(_fd)

            if fd not in self.fdpd_to_idx:
                self.fdpd_to_idx[fd] = {}
            self.fdpd_to_idx[fd][pd] = idx

            data = self._read(fd, pd)
            # if pd == 1:
            #     print(f'fd: {fd}, pd:{pd}, read data:{data}')
            data = np.frombuffer(data, np.uint8, PAGE_SIZE).copy()
            # print(f'after frombuffer data:{data}')
            self.pages[idx] = data

        self.idx_to_fdpd[idx] = (fd, pd)
        self.lru.access(idx)
        return data