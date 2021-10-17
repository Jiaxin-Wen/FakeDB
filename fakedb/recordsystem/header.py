import json
import numpy as np
from ..config import PAGE_SIZE

class Header:
    '''
    文件头
    '''
    def __init__(self, **kwargs):
        '''
        record_len: 每条记录的字节数
        record_capacity: 每页最大可存放的记录数
        record_num: 当前记录数
        page_num: 当前页数
        filename: 文件名
        bitmap_len: bitmap的字节数
        next_available_page: 下一个可以插入记录的页
        '''
        self.data = kwargs
        print('header = ', self.data)

    def serialize(self):
        '''
        导出一整页, 用0补位
        '''
        output = np.zeros(PAGE_SIZE, dtype=np.uint8)
        header_bytes = json.dumps(self.data).encode('utf-8')
        output[: len(header_bytes)] = list(header_bytes)
        return output

    @staticmethod
    def deserialize(self, data):
        '''
        恢复dict, 去掉补位的0
        '''
        header = json.loads(data.tobytes().decode('utf-8').rstrip('\0'))
        return header




if __name__ == '__main__':
    test = Header(a=2, b=3)
    