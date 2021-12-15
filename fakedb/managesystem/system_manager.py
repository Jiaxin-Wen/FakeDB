import os
import shutil
import traceback

from copy import copy

from antlr4 import InputStream, CommonTokenStream

from ..filesystem import FileManager
from ..recordsystem import RID, RecordManager, Record, get_all_records
from ..indexsystem import FileIndex, IndexManager
from ..metasystem import MetaManager, TableMeta
from ..parser import SQLLexer, SQLParser

from ..config import ROOT_DIR, TABLE_SUFFIX, INDEX_SUFFIX

from .utils import get_db_dir, get_table_path, get_index_path, get_db_tables, get_table_related_files, \
    compare_two_cols, compare_col_value, in_values, like_check, null_check
from .condition import ConditionKind, Condition
from .selector import SelectorKind, Selector

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
            print(f"syntax error: {e}")
            print(traceback.format_exc())
            
        try:
            res = self.visitor.visit(tree)
            print('final res: ', res)
        except Exception as e:
            print(f"execution error: {e}")
            print(traceback.format_exc())
        
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
        self.meta_manager.create_db(name)
        return f'create db: {name}'
        
    def drop_db(self, name):
        '''删除数据库'''
        # TODO: close file
        if name not in self.active_db:
            raise Exception(f"Can't drop non-existing database {name}")
        self.index_manager.close_index(name)
        self.meta_manager.drop_db(name)
        
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
        return f'current db change to : {self.current_db}'
        
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
        self.record_manager.create_file(get_table_path(self.current_db, tablemeta.name), tablemeta.get_record_size())
        # self.file_manager.create_file(get_table_path(self.current_db, tablemeta.name))
        return f'db = {self.current_db}, create table = {tablemeta}'
    
    def drop_table(self, name):
        '''删除表'''
        if self.current_db is None:
            raise Exception(f"Please use database first to drop table")
        self.meta_manager.drop_table(name)
        for file in get_table_related_files(self.current_db, name): # 删除表相关的文件
            self.file_manager.close_file(file)
            self.file_manager.remove_file(file)
        return f'drop table: {name} from db: {self.current_db}'
                   
    def describe_table(self, name):
        '''展示一张表'''
        table_meta = self.meta_manager.get_table(name)
        return table_meta.get_description()

    def filter_records_by_index(self, table_name, table_meta, conditions):
        results = set()
        for condition in conditions:
            if condition.kind != ConditionKind.Compare:
                continue
            if condition.table_name and condition.table_name != table_name:
                continue
            col_name = condition.col_name
            if table_meta.has_index(col_name):
                value = condition.value
                if value is not None:
                    value = int(condition.value)
                    l, h = -1e10, 1e10
                    operator = condition.operator
                    if operator == '<>':
                        continue
                    if operator == '=':
                        l = value
                        h = value
                    elif operator == '<':
                        h = value - 1
                    elif operator == '>':
                        l = value + 1
                    elif operator == '<=':
                        h = value
                    elif operator == '>=':
                        l = value

                    root_id = table_meta.indexes[col_name]
                    index_file_path = get_index_path(self.current_db, table_name, col_name)
                    index = self.index_manager.open_index(index_file_path, root_id)
                    rids = index.rangeSearch(l, h)
                    if rids is not None:
                        results &= set(rids)

        return results if len(results) > 0 else None

    def get_condition_func(self, condition: Condition, table_meta: TableMeta):
        if condition.table_name and condition.table_name != table_meta.name:
            return None

        col_name = condition.col_name
        col_idx = table_meta.get_col_idx(col_name)
        if col_idx is None:
            raise Exception(f'{condition.col_name} not in table {table_meta.name}!')

        col_kind = table_meta.column_dict[col_name].kind
        cond_kind = condition.kind
        if cond_kind == ConditionKind.Compare:
            if condition.col_name2:
                if condition.table_name2 != condition.table_name:
                    return None
                col_idx2 = table_meta.get_col_idx(condition.col_name2)
                return compare_two_cols(col_idx, col_idx2, condition.operator)
            else:
                value = condition.value
                if col_kind in ['INT', 'FLOAT'] and not isinstance(value, (int, float)):
                    raise Exception(f'col_kind is {col_kind} but {value} is not that type!')

                elif col_kind == 'VARCHAR' and not isinstance(value, str):
                    raise Exception(f'col_kind is {col_kind} but {value} is not that type!')

                return compare_col_value(col_idx, value, condition.operator)

        elif cond_kind == ConditionKind.In:
            return in_values(col_idx, condition.value)

        elif cond_kind == ConditionKind.Like:
            assert col_kind == 'VARCHAR', f'col_kind {col_kind} does not support LIKE!'
            return like_check(col_idx, condition.value)

        elif cond_kind == ConditionKind.IsNull:
            assert isinstance(condition.value, bool), f'{condition.value} is not bool value!'
            return null_check(col_idx, condition.value)

        else:
            return None

    def search_records_using_indexes(self, table_name, conditions):
        """
        :param table_name:
        :param conditions:
        :return: 满足条件的records和它们的values
        """
        table_meta = self.meta_manager.get_table(table_name)
        table_path = get_table_path(self.current_db, table_name)
        self.record_manager.open_file(table_path)
        index_filter_rids = self.filter_records_by_index(table_name, table_meta, conditions)
        if index_filter_rids is None:
            all_records = get_all_records(self.record_manager)
        else:
            all_records = list(map(self.record_manager.get_record, index_filter_rids))

        records = []
        values = []

        condition_funcs = []
        for condition in conditions:
            func = self.get_condition_func(condition, table_meta)
            if func is not None:
                condition_funcs.append(func)

        for record in all_records:
            record_values = table_meta.load_record(record.data)
            flag = True
            for func in condition_funcs:
                if not func(record_values):
                    flag = False
                    break
            if flag:
                records.append(record)
                values.append(record_values)

        return records, values
    
    def _insert_index(self, table_meta, value_list, rid):
        '''内部接口, 插入行后更新索引文件'''
        for col, rid in table_meta.indexes.items():
            index_path = get_index_path(self.current_db, table_meta.name, col)
            index = self.index_manager.open_index(index_path, rid)
            col_id = table_meta.get_col_idx(col)
            # FIXME: 对None值的处理
            if value_list[col_id] is not None:
                index.insert(value_list[col_id], rid) 
                
    def _delete_index(self, table_meta, value_list, rid):
        '''内部接口, 删除行后更新索引文件'''
        for col, rid in table_meta.indexes.items():
            index_path = get_index_path(self.current_db, table_meta.name, col)
            index = self.index_manager.open_index(index_path, rid)
            col_id = table_meta.get_col_idx(col)
            # FIXME: 对None值的处理
            if value_list[col_id] is not None:
                index.remove(value_list[col_id], rid)
            
    def insert_record(self, table, value_list):
        '''在表中插入行'''
        if self.current_db is None:
            raise Exception(f"Please use database first to insert record")
        print(f'insert, table = {table}, value = {value_list}')
        table_meta = self.meta_manager.get_table(table)       
        data = table_meta.build_record(value_list) # 字节序列
                
        # TODO: 检查约束
        
        table_path = get_table_path(self.current_db, table)
        self.record_manager.open_file(table_path)
        rid = self.record_manager.insert_record(data)
        
        self._insert_index(table_meta, value_list, rid)
        
    def delete_record(self, table, conditions):
        '''在表中根据条件删除行'''
        # for i in conditions:
        #     print(i)
        if self.current_db is None:
            raise Exception(f"Please use database first to delete record")
        table_meta = self.meta_manager.get_table(table)
        table_path = get_table_path(self.current_db, table)
        records, values = self.search_records_using_indexes(table, conditions)
        self.record_manager.open_file(table_path)
        for record, value in zip(records, values):
            rid = record.rid
            # TODO: 检查约束
            self.record_manager.delete_record(rid)
            self._delete_index(table_meta, value, rid)
            
        return 'delete'
    
    def update_record(self, table, conditions, update_info):
        '''在表中更新record'''
        # print('table = ', table)
        # for i in conditions:
        #     print(i)
        # print('update_info = ', update_info)
        table_meta = self.meta_manager.get_table(table)
        records, record_values = self.search_records_using_indexes(table, conditions) # 根据condition找到的record和原始value
        
        self.record_manager.open_file(get_table_path(self.current_db, table))
        
        # print('records = ', records)
        # print('record_values = ', record_values)
        for record, ori_value_list in zip(records, record_values):
            new_value_list = copy(ori_value_list)
            for col, new_value in update_info.items():
                index = table_meta.get_col_idx(col)
                new_value_list[index] = new_value # 维护更新后的record
            
            # TODO: 检查约束
            
            # 更新record
            data = table_meta.build_record(new_value_list)
            self.record_manager.update_record(record.rid, data)
                        
            # 维护index 先删除再添加
            self._delete_index(table_meta, ori_value_list, record.rid)
            self._insert_index(table_meta, new_value_list, record.rid)
        
        return "update record"
    
    def select_records(self, selectors, tables, conditions, group_by, limit, offset):
        '''
        select语句
        TODO: 
        - group by, limit, offset
        '''
        for i in selectors:
            print(i)
        for i in conditions:
            print(i)
            
        # print('tables = ', tables)
        assert len(tables) == 1 # 暂时不支持group_by
        table = tables[0]
        table_meta = self.meta_manager.get_table(table)
        # print('group by = ', group_by)
        # print('limit = ', limit)
        # print('offset = ', offset)
        
        if self.current_db is None:
            raise Exception(f"Please use database first to select records")

        _, value_list = self.search_records_using_indexes(table, conditions)
        if len(selectors) == 1 and selectors[0].kind == SelectorKind.All: # select *
            return value_list
        else: 
            res = {}
            for selector in selectors:
                col = selector.col_name
                selected_value_list = []
                if col == '*': # Count (*)
                    selected_value_list = [0] * len(value_list)
                else:
                    col_idx = table_meta.get_col_idx(col)
                    selected_value_list = [i[col_idx] for i in value_list]
                
                selected_value_list = selector(selected_value_list)
                
                res[str(selector)] = selected_value_list      
            return res                

        raise Exception("not implemented branch")
    
    def add_index(self, table, col):
        '''添加索引'''
        table_meta = self.meta_manager.get_table(table)
        if not table_meta.has_column(col): # 判断列是否在表中
            return f"{table} has no column named {col}"
        if table_meta.has_index(col): # 判断该列是否已创建过索引
            return f"{table}.{col} has created index"
        
        # 创建index文件
        index_path = get_index_path(self.current_db, table, col)
        index = self.index_manager.create_index(index_path)
        
        table_meta.create_index(col, index.root_id)
        
        # 初始化
        col_idx = table_meta.get_col_idx(col)
        self.record_manager.open_file(get_table_path(self.current_db, table))
        records = get_all_records(self.record_manager)
        for record in records:
            value = table_meta.load_record(record.data)
            index.insert(value[col_idx], record.rid)
        return f"add index on {table}.{col}" 
    
    def drop_index(self, table, col):
        '''删除索引'''
        table_meta = self.meta_manager.get_table(table)
        if not table_meta.has_column(col): # 判断列是否在表中
            return f"{table} has no column named {col}"
        if not table_meta.has_index(col): # 判断该列是否已创建过索引
            return f"{table}.{col} has not created index"
        table_meta.drop_index(col)
        
        index_path = get_index_path(self.current_db, table, col)
        self.file_manager.close_file(index_path)
        self.file_manager.remove_file(index_path)
        return f'drop index: {table}.{col}'
    
    def show_indexes(self):
        '''打印数据库中的所有索引'''
        return self.meta_manager.get_indexes_description()
    
    def add_primary_key(self, table, primary_key_list):
        '''添加主键'''
        table_meta = self.meta_manager.get_table(table)
        for key in primary_key_list:
            table_meta.add_primary(key)
            self.add_index(table, key)
        return f'add primariy key: {primary_key_list} in {table}'  
    
    def drop_primary_key(self, table, primary_key):
        '''删除主键''' 
        table_meta = self.meta_manager.get_table(table)
        if primary_key is not None:
            self.drop_index(table, primary_key)
            return f'drop primary key: {table}.{primary_key}'
        else:
            primary_keys = table_meta.primary
            for key in primary_keys:
                self.drop_index(table, key)
            return f'drop all primary keys in {table}: {",".join(primary_keys)}'
    
    def add_foreign_key(self, table, foreign_table, key, foreign_key, foreign_name):
        '''添加外键'''
        table_meta = self.meta_manager.get_table(table)
        table_meta.add_foreign(key, f"{foreign_table}.{foreign_key}")
        return self.add_index(foreign_table, foreign_key)
    
    def drop_foreign_key(self, table, key):
        '''删除外键'''
        table_meta = self.meta_manager.get_table(table)
        foreign_info = table_meta.foreigns[key]
        foreign_table, foreign_key = foreign_info.split('.')
        return self.drop_index(foreign_table, foreign_key)        

    def add_unique(self, table, col):
        table_meta = self.meta_manager.get_table(table)
        table_meta.add_unique(col)
        return f'add unique on {table}.{col}'

    def shutdown(self):
        '''退出'''
        self.file_manager.shutdown()
        self.record_manager.shutdown()
        self.index_manager.shutdown()
        self.meta_manager.shutdown()
    
    
    
    