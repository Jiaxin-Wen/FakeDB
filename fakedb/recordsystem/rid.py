class RID:
    
    def __init__(self, page_id, slot_id):
        '''
        page_id: 页号
        slot_id: 槽号
        通过(文件路径, 页号, 槽号)就可以定位一条记录
        '''
        self.page_id = page_id
        self.slot_id = slot_id
    