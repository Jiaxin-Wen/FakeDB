'''
索引管理模块:
直接从索引文件中读取数据来加速对特定列的查询。
采用B+树
'''

from .fileindex import FileIndex
from .index_manager import IndexManager