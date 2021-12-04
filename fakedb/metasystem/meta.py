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
        for meta in columnmetas:
            self.column_dict[meta.name] = meta

    def add_column(self, columnmeta):
        if columnmeta.name in self.column_dict:
            raise Exception(f'column {columnmeta.name} cannot be added because it exists!')

        self.column_dict[columnmeta.name] = columnmeta

    def drop_column(self, name):
        if name not in self.column_dict:
            raise Exception(f'column {name} cannot be dropped because it does not exist!')
        self.column_dict.pop(name)

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
        return '\n'.join([v.get_description() for v in self.column_dict.values()])

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
                    strbytes = tuple(value.encode())
                    if len(strbytes) > siz:
                        raise Exception(
                            f'VARCHAR {value} has length {len(strbytes)}, which is larger than maximum size {siz}')
                    res[pos: pos + len(strbytes)] = strbytes
                else:
                    if columnmeta.kind == 'INT':
                        intbytes = tuple(struct.pack('q', value))
                        res[pos: pos + siz] = intbytes
                    elif columnmeta.kind == 'FLOAT':
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
                    value = data[pos: pos+siz].tobytes().rstrip(b'\x00').decode('utf-8')
                else:
                    if kind == 'INT':
                        value = struct.unpack('q', data[pos: pos+siz])[0]
                    elif kind == 'FLOAT':
                        value = struct.unpack('d', data[pos: pos+siz])[0]
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
