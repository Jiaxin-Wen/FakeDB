import os
import shutil
import traceback

from collections import defaultdict
from itertools import product
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

    def cond_join(self, table2records, conditions):
        res = []
        table2idx = {}
        for i, table in enumerate(table2records):
            table2idx[table] = i
        table_metas = {table_name: self.meta_manager.get_table(table_name) for table_name in table2records}

        for tup in product(*table2records.values()):
            flag = True
            for cond in conditions:
                if cond.table_name2 is None:
                    continue
                assert cond.kind == ConditionKind.Compare, f'expected Compare for two table columns, but got {cond.kind}'
                table_meta = table_metas[cond.table_name]
                col_idx = table_meta.get_col_idx(cond.col_name)
                table_idx = table2idx[cond.table_name]
                val = tup[table_idx][col_idx]

                table_meta2 = table_metas[cond.table_name2]
                col_idx2 = table_meta2.get_col_idx(cond.col_name2)
                table_idx2 = table2idx[cond.table_name2]
                val2 = tup[table_idx2][col_idx2]

                op = cond.operator
                if op == '=':
                    if val != val2:
                        flag = False
                        break
                elif op == '<>':
                    if val == val2:
                        flag = False
                        break
                else:
                    if not eval(f'{val}{op}{val2}'):
                        flag = False
                        break

            if flag:
                res.append({table_name: value_list for value_list, table_name in zip(tup, table2records)})

        return res

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
                if value is None:
                    return lambda x: True

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

    def check_constraints(self, tablemeta, values, old_record=None, delete=False):
        if delete:
            if not self.check_ref_foreign(tablemeta, values, old_record, delete):
                raise Exception(f'violate foreign constraint')
        else:
            if not self.check_null(tablemeta, values):
                raise Exception(f'{values} violate Null constraint')

            if not self.check_primary(tablemeta, values, old_record):
                raise Exception(f'{values} violate primary constraint')

            if not self.check_unique(tablemeta, values, old_record):
                raise Exception(f'{values} violate unique constraint')

            if not self.check_foreign(tablemeta, values):
                raise Exception(f'{values} violate foreign constraint')

            if old_record is not None:
                if not self.check_ref_foreign(tablemeta, values, old_record, delete):
                    raise Exception(f'violate foreign constraint')

    def check_ref_foreign(self, tablemeta, values, old_record=None, delete=False):
        if delete:
            flag = True
            for keys, tab_cols in tablemeta.ref_foreigns_alias.values():
                conditions = []
                tab = None
                for key, tab_col in zip(keys, tab_cols):
                    idx = tablemeta.get_col_idx(key)
                    value = values[idx]
                    tab, col = tab_col.split('.')
                    conditions.append(Condition(ConditionKind.Compare, tab, col, '=', value))
                records, vals = self.search_records_using_indexes(tab, conditions)
                if len(records) > 0:
                    flag = False
                    break

            return flag

        else:
            flag = True
            old_values = tablemeta.load_record(old_record.data)
            for keys, tab_cols in tablemeta.ref_foreigns_alias.values():
                value_all_same = True
                print(values, old_values)
                for key in keys:
                    idx = tablemeta.get_col_idx(key)
                    if values[idx] != old_values[idx]:
                        value_all_same = False
                        break
                if value_all_same:
                    continue
                conditions = []
                tab = None
                for key, tab_col in zip(keys, tab_cols):
                    idx = tablemeta.get_col_idx(key)
                    value = old_values[idx]
                    tab, col = tab_col.split('.')
                    conditions.append(Condition(ConditionKind.Compare, tab, col, '=', value))

                records, vals = self.search_records_using_indexes(tab, conditions)
                if len(records) > 0:
                    flag = False
                    break
            print(f'in check_ref_foreign flag:{flag}')
            return flag

    def check_null(self, tablemeta, values):
        flag = True
        for i, colmeta in enumerate(tablemeta.column_dict.values()):
            if not colmeta.null:
                if values[i] is None:
                    flag = False
                    break

        return flag

    def check_unique(self, tablemeta, values, old_record=None):
        """
        :param tablemeta:
        :param values:
        :param old_record: 为None表示插入的情况，如果不为None，表示update之前旧的record
        :return: True if no problem
        """
        flag = True
        for key in tablemeta.uniques:
            idx = tablemeta.get_col_idx(key)
            value = values[idx]
            conditions = [Condition(ConditionKind.Compare, tablemeta.name, key, '=', value)]
            records, vals = self.search_records_using_indexes(tablemeta.name, conditions)
            if old_record is None:
                if len(records) > 0:
                    flag = False
                    break
            else:
                if len(records) > 1:
                    flag = False
                    break
                elif len(records) == 1:
                    if records[0].rid != old_record.rid:
                        flag = False
                        break
        return flag

    def check_primary(self, tablemeta, values, old_record=None):
        """

        :param tablemeta:
        :param values:
        :param old_record:
        :return: True if no problem
        """
        if not tablemeta.primary:
            return True
        # check null first
        for key in tablemeta.primary:
            if values[tablemeta.get_col_idx(key)] is None:
                return False

        # check duplicate
        conditions = [Condition(ConditionKind.Compare, tablemeta.name, key, '=', values[tablemeta.get_col_idx(key)]) for key in tablemeta.primary]
        records, vals = self.search_records_using_indexes(tablemeta.name, conditions)
        if len(records) > 1:
            return False
        if len(records) == 0:
            return True
        # len(records) == 1
        if old_record is None:
            return False
        return records[0].rid == old_record.rid

    def check_foreign(self, tablemeta, values):
        """

        :param tablemeta:
        :param values:
        :return: True if no problem
        """
        flag = True
        for keys, tab_cols in tablemeta.foreigns_alias.values():
            conditions = []
            tab = None
            for key, tab_col in zip(keys, tab_cols):
                idx = tablemeta.get_col_idx(key)
                value = values[idx]
                tab, col = tab_col.split('.')
                conditions.append(Condition(ConditionKind.Compare, tab, col, '=', value))

            records, vals = self.search_records_using_indexes(tab, conditions)
            if len(records) == 0:
                flag = False
                break

        return flag
    
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
                
        self.check_constraints(table_meta, value_list, old_record=None)
        
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
            self.check_constraints(table_meta, value, record, delete=True)
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
            
            self.check_constraints(table_meta, new_value_list, record)
            
            # 更新record
            data = table_meta.build_record(new_value_list)
            self.record_manager.update_record(record.rid, data)
                        
            # 维护index 先删除再添加
            self._delete_index(table_meta, ori_value_list, record.rid)
            self._insert_index(table_meta, new_value_list, record.rid)
        
        return "update record"
    
    def select_records(self, selectors, tables, conditions, group_by, limit, offset):
        data = self._select_records(selectors, tables, conditions, group_by)
        print('ori select records = ', data)
        print('limit = ', limit)
        print('offset = ', offset)
        if limit is None:
            return data[offset:]
        else:
            return data[offset: offset + limit]
    
    def _select_records(self, selectors, tables, conditions, group_by):
        '''
        select语句
        TODO: 
        join
        '''
        for i in selectors:
            print(i)
        for i in conditions:
            print(i)
            
        print('tables = ', tables)
        # assert len(tables) == 1 # 暂时不支持group_by
        print('group by = ', group_by)
        
        if self.current_db is None:
            raise Exception(f"Please use database first to select records")

        # table to value_list
        value_dict = {table: self.search_records_using_indexes(table, conditions)[-1] for table in tables}
        print('value_dict = ', value_dict)
        # if len(tables) > 1: # join
        value_list_dict = self.cond_join(value_dict, conditions) # list of dict
        # else:
            # value_list = value_dict[tables[0]]
        print('valud list dict = ', value_list_dict)
        # TODO: 多表condition的结合
        # _, value_list = self.search_records_using_indexes(table, conditions)
        selector_kinds = set(selector.kind for selector in selectors)
        if group_by[-1] is None and SelectorKind.Field in selector_kinds and len(selector_kinds) > 1:
            # 只有使用groupby时才能同时出现field和聚集函数的select
            raise Exception("should use group by")
        
        def get_value_list(table):
            return [i[table] for i in value_list_dict]
        
        if group_by[-1] is not None: # group by
            group_table, group_col = group_by
            table_meta = self.meta_manager.get_table(group_table)
            group_value_dict = defaultdict(list) # 按group_by的列的值 构造dict
            group_idx = table_meta.get_col_idx(group_col)
            value_list = get_value_list(group_table)
            for i in value_list:
                key = i[group_table][group_idx]
                group_value_dict[key].append(i)
            
            group_res = []
            for i, tmp_value_list in group_value_dict.items():
                tmp_res = []
                for selector in selectors:
                    col = selector.col_name
                    selected_value_list = []
                    if col == '*': # Count (*)
                        selected_value_list = [0] * len(value_list)
                    else:
                        col_idx = table_meta.get_col_idx(col)
                        selected_value_list = [i[col_idx] for i in tmp_value_list]
                    
                    selected_value_list = selector(selected_value_list)
                    tmp_res.append(selected_value_list)
                group_res.append(tmp_res)
            return group_res
        else:
            if len(selectors) == 1 and selectors[0].kind == SelectorKind.All: # select *
                return get_value_list(tables[0])
            else: 
                res = []
                for selector in selectors:
                    col = selector.col_name
                    table = selector.table_name
                    table_meta = self.meta_manager.get_table(table)
                    value_list = get_value_list(table)
                    selected_value_list = []
                    if col == '*': # Count (*)
                        selected_value_list = [0] * len(value_list)
                    else:
                        col_idx = table_meta.get_col_idx(col)
                        selected_value_list = [i[col_idx] for i in value_list]
                    
                    selected_value_list = selector(selected_value_list)
                    
                    # print(f'selector = {str(selector)}, ori value list = {value_list}, selected value list = {selected_value_list}')
                    res.append(selected_value_list)
                
                # print('ori res = ', res)
                first_len = len(res[0])
                for i in res:
                    try:
                        assert len(i) == first_len
                    except:
                        raise "多表join, 每张表的行数应该相同"
                res = [[row[i] for row in res] for i in range(len(res[0]))]
                # print('new res = ', res)
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
        if primary_key_list is None: # create table时未指定主键
            return
        table_meta = self.meta_manager.get_table(table)
        for key in primary_key_list:
            table_meta.add_primary(key)
            self.add_index(table, key)
        return f'add primariy key: {primary_key_list} in {table}'  
    
    def drop_primary_key(self, table, primary_key):
        '''删除主键''' 
        table_meta = self.meta_manager.get_table(table)
        primary_keys = copy(table_meta.primary)
        for col in primary_keys: # 删掉所有主键列的index
            self.drop_index(table, col)
        table_meta.drop_primary() # 删除主键
        print('primary keys = ', primary_keys)
        return f'drop primary key: {table}.{",".join(primary_keys)}'
    
    def add_foreign_key(self, table, foreign_table, key, foreign_key, foreign_name):
        '''添加外键'''
        table_meta = self.meta_manager.get_table(table)
        # table_meta.add_foreign(key, f"{foreign_table}.{foreign_key}")
        table_meta.add_foreign(tuple(key), tuple(f"{foreign_table}.{i}" for i in foreign_key), foreign_name)
        ref_table_meta = self.meta_manager.get_table(foreign_table)
        ref_table_meta.add_ref_foreign(tuple(foreign_key), tuple(f"{table}.{i}" for i in key), foreign_name)
        # ref_table_meta.add_ref_foreign(foreign_key, f'{table}.{key}')
        for i in foreign_key:
            self.add_index(foreign_table, i)
        return f"add foreign key, alias = {foreign_name}, ori = {table}.{','.join(key)}, ref = {foreign_table}.{','.join(foreign_key)}"
    
    def drop_foreign_key(self, table, foreign_name):
        '''删除外键'''
        table_meta = self.meta_manager.get_table(table) 
        foreign_info = table_meta.foreigns_alias[foreign_name][-1]
        foreign_table = foreign_info[0].split('.')[0]
        foreign_key = [i.split('.')[-1] for i in foreign_info]
        table_meta.remove_foreign(foreign_name)
        ref_table_meta = self.meta_manager.get_table(foreign_table)
        ref_table_meta.remove_ref_foreign(foreign_name)
        for i in foreign_key:
            self.drop_index(foreign_table, i)
        return f"drop foreign key, alias = {foreign_name}, ref = {foreign_table}.{','.join(foreign_key)}"

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
    
    
    
    