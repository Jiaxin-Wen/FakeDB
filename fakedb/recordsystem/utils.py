from ..config import PAGE_SIZE


def get_record_size(record_len):
    '''
    计算每页最多能存储的记录条数
    考虑bitmap的占用
    '''

    size = (PAGE_SIZE * 8) // (1 + record_len * 8)
    assert ((size + 7) // 8) + size * record_len <= PAGE_SIZE
    return size