from ..config import PAGE_SIZE


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