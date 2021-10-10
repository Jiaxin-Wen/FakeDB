from .rid import RID


class Record:

    def __init__(self, rid, data):
        '''
        rid: 记录的id
        data: 记录的数据
        '''
        self.rid = rid
        self.data = data
