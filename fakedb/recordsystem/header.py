import json
import numpy as np
from ..config import PAGE_SIZE

class Header:
    '''
    文件头
    '''
    def __init__(self, **kwargs):
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
    