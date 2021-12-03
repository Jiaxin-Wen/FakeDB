import os



from ..filesystem import FileManager
from ..recordsystem import RID, RecordManager, Record
from ..indexsystem import FileIndex, IndexManager
from ..metasystem import MetaManager
from ..parser import SQLLexer, SQLParser


from ..config import ROOT_DIR


class SystemManager:
    '''
    SystemManager
    '''
    def __init__(self):
        # TODO: 把base_path维护成一个
        self.file_manager = FileManager()
        self.record_manager = RecordManager(self.file_manager)
        self.index_manager = IndexManager(self.file_manager)
        
        if not os.path.exists(ROOT_DIR):
            os.mkdir(ROOT_DIR)
        
    
    def create_db(self, name):
        '''创建数据库'''
        pass
    
    def drop_db(self, name):
        '''删除数据库'''
        pass
    
    def use_db(self, name):
        '''选择数据库'''
        pass
    
    def show_tables(self):
        '''展示数据库中的所有表'''
        pass
    
    def create_table(self, name):
        '''创建表'''
        pass
    
    def drop_table(self, name):
        '''删除表'''
        pass
    
    def describe_table(self, name):
        '''展示一张表'''
        pass
    
    
    
    