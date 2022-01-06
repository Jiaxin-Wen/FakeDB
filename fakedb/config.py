'''
常数定义
'''


PAGE_SIZE = 8192 # 采用文档中的设定, 每页8192字节
PAGE_SIZE_BITS = 13 # 8192字节为13位

CACHE_SIZE = 6 # 采用文档中的设定, 缓存有60k页

NEXT_AVAILABLE_PAGE_OFFSET = 0 # 每一页中记录下个空闲页的id
NEXT_AVAILABLE_PAGE_SIZE = 4 # 每一页中花几个字节记录下个空闲页的id

BITMAP_START_OFFSET = 4 # bitmap的起始字节数

NULL_VALUE = -1e10

# 文件命名
TABLE_SUFFIX = '.table' # 表文件后缀
INDEX_SUFFIX = '.index' # 索引文件后缀
META_SUFFIX = '.meta'

# 存储路径
ROOT_DIR = 'log'