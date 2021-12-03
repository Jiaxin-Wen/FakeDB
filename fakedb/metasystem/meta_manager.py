# from ..config import root_dir

class MetaManager:
    def __init__(self, fm, root_dir):
        self.fm = fm
        self.root_dir = root_dir
        self.db_dict = {}
        self.current_db = None

    def drop_db(self, name):
        if name not in self.db_dict:
            raise Exception(f'database {name} does not exist!')

        self.db_dict.pop(name)

    def use_db(self, name):
        if name not in self.db_dict:
            raise Exception(f'database {name} does not exist!')
        self.current_db = self.db_dict[name]

    def create_table(self, tablemeta):
        self.current_db.create_table(tablemeta)

    def drop_table(self, name):
        self.current_db.drop_table(name)

    def get_table(self, name):
        return self.current_db.get_table(name)