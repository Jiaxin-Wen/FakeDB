import os
import shutil



from ..filesystem import FileManager
from ..recordsystem import RID, RecordManager, Record
from ..indexsystem import FileIndex, IndexManager
from ..metasystem import MetaManager
from ..parser import SQLLexer, SQLParser

from ..config import ROOT_DIR

from .utils import get_db_dir, get_db_tables


class SystemManager:
    '''
    SystemManager
    '''
    def __init__(self):
        # TODO: 把base_path维护成一个
        self.file_manager = FileManager()
        self.record_manager = RecordManager(self.file_manager)
        self.index_manager = IndexManager(self.file_manager)
        
        self.init_db()

        self.current_db = None # 当前正在使用的db
        
    def init_db(self):
        '''从目录下恢复状态'''
        if not os.path.exists(ROOT_DIR):
            os.mkdir(ROOT_DIR)
            
        self.active_db = set()
        for file in os.listdir(ROOT_DIR):
            self.active_db.add(file)        
    
    def create_db(self, name):
        '''创建数据库'''
        if name in self.active_db:
            raise Exception(f"Can't create existing database {name}")
        os.mkdir(get_db_dir(name))
        self.active_db.add(name)
        
    def drop_db(self, name):
        '''删除数据库'''
        if name not in self.active_db:
            raise Exception(f"Can't drop non-existing database {name}")
        self.index_manager.close_index(name)
        
        db_dir = get_db_dir(name)
        # TODO: close_meta
        
        # FIXME： 确认一下是否会存在问题
        # 直接删除数据库目录
        shutil.rmtree(db_dir)
        
        self.active_db.remove(name)
        
        if self.current_db == name:
            self.current_db = None
        
    def use_db(self, name):
        '''选择数据库'''
        if name not in self.active_db:
            raise Exception(f"Can't use non-existing database {name}")
        self.current_db = name
    
    def show_tables(self):
        '''展示数据库中的所有表'''
        if self.current_db is None:
            raise Exception(f"Please using database first to show tables")
        return get_db_tables(self.current_db)
    
    def create_table(self, name):
        '''创建表'''
        if self.current_db is None:
            raise Exception(f"Please using database first to create table")
        
    
    def drop_table(self, name):
        '''删除表'''
        pass
    
    def describe_table(self, name):
        '''展示一张表'''
        pass
    
    
    
    