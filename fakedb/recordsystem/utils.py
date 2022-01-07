from ..config import PAGE_SIZE
from .rid import RID
import numpy as np


def get_all_records(record_manager):
    page_num = record_manager.header.page_num
    res = []
    for page_id in range(1, page_num):
        page = record_manager.get_page(page_id)
        # if page_id == 1:
        #     print(f'page 1:{page.tobytes()}')
        bitmap = record_manager.get_bitmap(page)
        for slot_id in np.where(bitmap == 0)[0]:
            record = record_manager.get_record(RID(page_id, slot_id))
            res.append(record)
        # if page_id == 1:
        #     print(f'in for, res 0:{res[0].data.tobytes()}')

    # if res:
    #     print(f'res 0:{res[0].data.tobytes()}')
    return res

def get_record_capacity(record_len):
    '''
    计算每页最多能存储的记录条数
    考虑bitmap的占用
    
    设为x, 则有
    (x + 7) // 8 + x * record_len <= PAGE_SIZE
    x + 7 + x * record_len * 8 <= PAGE_SIZE * 8
    '''

    size = (PAGE_SIZE * 8 - 7) // (1 + record_len * 8)
    assert ((size + 7) // 8) + size * record_len <= PAGE_SIZE
    return size


def get_bitmap_len(record_capacity):
    '''
    计算bitmap需要的字节数
    '''
    bitmap_len = (record_capacity + 7) // 8 # 取上整
    return bitmap_len