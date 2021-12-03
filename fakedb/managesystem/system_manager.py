import os
import shutil

from antlr4 import InputStream, CommonTokenStream

from ..filesystem import FileManager
from ..recordsystem import RID, RecordManager, Record
from ..indexsystem import FileIndex, IndexManager
from ..metasystem import MetaManager
from ..parser import SQLLexer, SQLParser

from ..config import ROOT_DIR

from .utils import get_db_dir, get_db_tables, get_table_related_files


class SystemManager:
    '''
    SystemManager
    '''
    def __init__(self, visitor):
               
        self.file_manager = FileManager()
        self.record_manager = RecordManager(self.file_manager)
        self.index_manager = IndexManager(self.file_manager)
        self.meta_manager = MetaManager(self.file_manager)
        
        self.init_db()
        self.current_db = None # 当前正在使用的db
        
        self.visitor = visitor
        self.visitor.manager = self
        
    def init_db(self):
        '''从目录下恢复状态'''
        if not os.path.exists(ROOT_DIR):
            os.mkdir(ROOT_DIR)
            
        self.active_db = set()
        for file in os.listdir(ROOT_DIR):
            self.active_db.add(file)  
            
        self.meta_manager.load_alldbs()
            
    def execute(self, query):
        '''
        封装给外部调用的主接口
        接受一条sql query语句
        返回执行结果
        ''' 
        
        input_stream = InputStream(query)
        lexer = SQLLexer(input_stream)
        tokens = CommonTokenStream(lexer)
        parser = SQLParser(tokens)
        try:
            tree = parser.program()
        except Exception as e:
            return f"syntax error: {e}"
        try:
            res = self.visitor.visit(tree)
            print(res)
        except Exception as e:
            return f"execution error: {e}"
        
    def show_dbs(self):
        '''打印全部数据库'''
        print('dbs = ', self.meta_manager.get_databases_description())
        return self.meta_manager.get_databases_description()
            
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
        self.meta_manager.use_db(name) # 维护meta_manager中的current_db
    
    def show_tables(self):
        '''展示数据库中的所有表'''
        if self.current_db is None:
            raise Exception(f"Please using database first to show tables")
        return get_db_tables(self.current_db)
    
    def create_table(self, tablemeta):
        '''创建表'''
        if self.current_db is None:
            raise Exception(f"Please using database first to create table")
        
        self.meta_manager.create_table(tablemeta)
    
    def drop_table(self, name):
        '''删除表'''
        if self.current_db is None:
            raise Exception(f"Please use database first to drop table")
        self.meta_manager.drop_table(name)
        for file in get_table_related_files(self.current_db, name): # 删除表相关的文件
            self.file_manager.remove_file(file)
                
    def describe_table(self, name):
        '''展示一张表'''
        table_meta = self.meta_manager.get_table(name)
        return table_meta.get_description()
    
    
    
    