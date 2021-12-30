import os
import re

from ..config import TABLE_SUFFIX, INDEX_SUFFIX
from ..config import ROOT_DIR


def compare_two_cols(col_idx, col_idx2, operator):
    if operator == '<>':
        return lambda x: x[col_idx] != x[col_idx2]
    elif operator == '=':
        return lambda x: x[col_idx] == x[col_idx2]
    else:
        return lambda x: eval(f'{x[col_idx]}{operator}{x[col_idx2]}')


def compare_col_value(col_idx, value, operator, col_null_true, value_null_true):
    if col_null_true:
        if operator == '=':
            return lambda x: x[col_idx] is None or x[col_idx] == value
        elif operator == '<>':
            return lambda x: x[col_idx] is None or x[col_idx] != value
        else:
            return lambda x: x[col_idx] is None or eval(f'{x[col_idx]}{operator}{value}')

    if value_null_true:
        if value is None:
            return lambda x: True
        else:
            return lambda x: False

    if value is None:
        return lambda x: False

    if operator == '=':
        return lambda x: x[col_idx] is not None and x[col_idx] == value
    elif operator == '<>':
        return lambda x: x[col_idx] is not None and x[col_idx] != value
    else:
        return lambda x: x[col_idx] is not None and eval(f'{x[col_idx]}{operator}{value}')


def in_values(col_idx, values):
    return lambda x: x[col_idx] in values


def sql_to_re_pattern(pattern):
    # TODO: CHECK
    pattern = pattern.replace('%%', '\r').replace('%?', '\n').replace('%_', '\0')
    pattern = re.escape(pattern)
    pattern = pattern.replace('%', '.*').replace(r'\?', '.').replace('_', '.')
    pattern = pattern.replace('\r', '%').replace('\n', r'\?').replace('\0', '_')
    return re.compile('^' + pattern + '$')


def like_check(col_idx, pattern):
    pattern = sql_to_re_pattern(pattern)
    return lambda x: pattern.match(x[col_idx])


def null_check(col_idx, is_null: bool):
    if is_null:
        return lambda x: x[col_idx] is None
    else:
        return lambda x: x[col_idx] is not None


def get_db_dir(name):
    '''返回数据库的存储目录(一个文件夹)'''
    return f"{ROOT_DIR}/{name}"


def get_table_path(db, table):
    '''返回数据库中一张表的路径'''
    return f"{ROOT_DIR}/{db}/{table}{TABLE_SUFFIX}"


def get_index_path(db, table, col):
    '''返回数据库中一张表索引文件的路径'''
    return f"{ROOT_DIR}/{db}/{table}_{col}{INDEX_SUFFIX}"


def get_db_tables(name):
    '''返回数据库下的所有表名'''
    db_dir = get_db_dir(name)
    tables = []
    for file in os.listdir(db_dir):
        if file.endswith(TABLE_SUFFIX):
            tables.append(file.split('.')[0])
    assert len(set(tables)) == len(tables)  # 无重名的表
    return tables


def get_table_related_files(db, table):
    '''返回数据库相关的所有文件, 包括.table, .index'''
    output = []
    for file in os.listdir(f"{ROOT_DIR}/{db}"):
        if file.startswith(table):
            output.append(f'{ROOT_DIR}/{db}/{file}')
    return output
