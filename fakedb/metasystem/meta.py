

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
        return ' '.join([self.name, f'{self.kind}{f"({self.siz})" if self.kind == "VARCHAR" else "" }', str(self.null), "", str(self.default), ""])



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

    def get_description(self):
        return '\n'.join([v.get_description() for v in self.column_dict.values()])



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



