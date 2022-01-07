import numpy as np
from math import ceil
import struct


class ColumnMeta:
    def __init__(self, name, kind, siz=None, null=False, default=None):
        self.name = name
        self.kind = kind
        self.siz = siz
        self.null = null
        self.default = default

    def get_siz(self):
        if self.kind != 'VARCHAR':
            return 8
        # FIXME: self.siz + 1?
        return self.siz

    def get_description(self):
        # Field, Type, Null, Key, Default, Extra
        return ' '.join(
            [self.name, f'{self.kind}{f"({self.siz})" if self.kind == "VARCHAR" else ""}', str(self.null), "",
             str(self.default), ""])


class TableMeta:
    def __init__(self, name, columnmetas):
        self.name = name
        self.column_dict = {}
        self.col_idx = {}
        for i, meta in enumerate(columnmetas):
            self.column_dict[meta.name] = meta
            self.col_idx[meta.name] = i
        self.indexes = {} # column_name to its index file's root page_id
        self.primary = set()
        self.foreigns_alias = {} # key值是alias
        self.uniques = set()
        self.ref_foreigns_alias = {} # key值是alias
        self.ref_foreigns = {} # key值是列名

    def __str__(self):
        return self.name


    def add_column(self, columnmeta):
        if columnmeta.name in self.column_dict:
            raise Exception(f'column {columnmeta.name} cannot be added because it exists!')

        self.column_dict[columnmeta.name] = columnmeta

    def drop_column(self, name):
        if name not in self.column_dict:
            raise Exception(f'column {name} cannot be dropped because it does not exist!')
        self.column_dict.pop(name)

    def has_column(self, name):
        return name in self.column_dict

    def get_col_idx(self, name):
        if name not in self.col_idx:
            raise Exception(f"column {name} does not exist")
        return self.col_idx.get(name, None)

    def has_index(self, colname):
        return colname in self.indexes

    def create_index(self, colname, page_id):
        self.indexes[colname] = page_id

    def drop_index(self, colname):
        if colname not in self.indexes:
            raise Exception(f'colomn {colname} does not have index!')
        self.indexes.pop(colname)

    def add_primary(self, colname):
        self.primary.add(colname)

    def drop_primary(self):
        self.primary.clear()
        # if colname in self.primary:
        #     self.primary.remove(colname)

    def add_foreign(self, colnames, foreigns, alias):
        # for colname, foreign in zip(colnames, foreigns):
        #     self.foreigns[colname] = foreign
        self.foreigns_alias[alias] = (colnames, foreigns)

    def remove_foreign(self, alias):
        if alias in self.foreigns_alias:
            # colnames = self.foreigns_alias[alias][0]
            # for colname in colnames:
            #     self.foreigns.pop(colname)
            self.foreigns_alias.pop(alias)

    def add_ref_foreign(self, colnames, foreigns, alias):
        self.ref_foreigns_alias[alias] = (colnames, foreigns)
        # for colname, foreign in zip(colnames, foreigns):
        #     self.ref_foreigns[colname] = foreign

    def remove_ref_foreign(self, alias):
        if alias in self.ref_foreigns_alias:
            # colnames = self.ref_foreigns_alias[alias][0]
            # for colname in colnames:
            #     self.ref_foreigns.pop(colname)
            self.ref_foreigns_alias.pop(alias)

    def add_unique(self, colname):
        self.uniques.add(colname)

    def get_record_null_bitmap_len(self):
        return ceil(len(self.column_dict) / 8)

    def get_record_size(self):
        res = 0
        # additional size for indicating null
        res += self.get_record_null_bitmap_len()
        for meta in self.column_dict.values():
            res += meta.get_siz()

        return res

    def get_description(self):
        return '\n'.join([v.get_description() for v in self.column_dict.values()]) + ';' + ' '.join([key for key in self.primary]) + ';' + ' '.join(
            [f'{foreign[0]} REFERENCES {foreign[1]}' for foreign in self.foreigns_alias.values()]
        ) + ';' + ' '.join(sorted([item for item in self.uniques])) + ';' + ' '.join(sorted([idx for idx in self.indexes])) + ';'

    def build_record(self, values):
        record_siz = self.get_record_size()
        res = np.zeros(record_siz, dtype=np.uint8)

        bitmap_len = self.get_record_null_bitmap_len()
        bitmap_data = np.unpackbits(np.array([0] * bitmap_len, dtype=np.uint8))
        pos = bitmap_len

        for i, (columnmeta, value) in enumerate(zip(self.column_dict.values(), values)):
            siz = columnmeta.get_siz()
            if value is None:
                bitmap_data[i] = 1
            else:
                if columnmeta.kind == 'VARCHAR':
                    if not isinstance(value, str):
                        raise Exception(f'{value} is not VARCHAR')
                    strbytes = tuple(value.encode())
                    if len(strbytes) > siz:
                        raise Exception(
                            f'VARCHAR {value} has length {len(strbytes)}, which is larger than maximum size {siz}')
                    res[pos: pos + len(strbytes)] = strbytes
                else:
                    if columnmeta.kind == 'INT':
                        if not isinstance(value, int):
                            raise Exception(f'{value} is not INT')
                        intbytes = tuple(struct.pack('q', value))
                        res[pos: pos + siz] = intbytes
                    elif columnmeta.kind == 'FLOAT':
                        if isinstance(value, str):
                            raise Exception(f'{value} is not float')
                        floatbytes = tuple(struct.pack('d', value))
                        res[pos: pos + siz] = floatbytes
                    else:
                        raise Exception(f'Wrong kind {columnmeta.kind}!')

            pos += siz

        res[:bitmap_len] = np.packbits(bitmap_data)
        return res

    def load_record(self, data):
        bitmap_len = self.get_record_null_bitmap_len()
        bitmap_data = np.unpackbits(data[:bitmap_len])

        values = []
        pos = bitmap_len
        for i, columnmeta in enumerate(self.column_dict.values()):
            siz = columnmeta.get_siz()
            if bitmap_data[i] == 1:
                values.append(None)
            else:
                kind = columnmeta.kind
                if kind == 'VARCHAR':
                    value = data[pos: pos + siz].tobytes().rstrip(b'\x00').decode('utf-8')
                else:
                    if kind == 'INT':
                        value = struct.unpack('q', data[pos: pos + siz])[0]
                    elif kind == 'FLOAT':
                        value = struct.unpack('d', data[pos: pos + siz])[0]
                    else:
                        raise Exception(f'Wrong kind {columnmeta.kind}!')
                values.append(value)

            pos += siz

        return values


class DbMeta:
    def __init__(self, name, tablemetas):
        self.name = name
        self.table_dict = {}
        for meta in tablemetas:
            self.table_dict[meta.name] = meta

    def get_indexes_description(self):
        res = []
        for tablemeta in self.table_dict.values():
            res.append(f'{tablemeta.name}: {list(tablemeta.indexes.keys())}')
        return '\n'.join(res)

    def create_table(self, tablemeta):
        if tablemeta.name in self.table_dict:
            raise Exception(f'table {tablemeta.name} cannot be created because it exists!')
        self.table_dict[tablemeta.name] = tablemeta

    def drop_table(self, name):
        if name not in self.table_dict:
            raise Exception(f'table {name} cannot be dropped because it does not exist!')
        self.table_dict.pop(name)

    def get_table(self, name):
        if name not in self.table_dict:
            raise Exception(f'table {name} does not exist!')
        return self.table_dict[name]
