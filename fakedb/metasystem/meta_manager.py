from ..config import ROOT_DIR, META_SUFFIX
import pickle
import os

class MetaManager:
    def __init__(self, fm):
        self.fm = fm
        self.db_dict = {}
        self.current_db = None

    # def writeback_db(self, name):
    #     if name not in self.db_dict:
    #         raise Exception(f'database {name} does not exist!')
    #     path = f'{ROOT_DIR}/{name}/{name}{META_SUFFIX}'
    #     with open(path, 'wb') as f:
    #         pickle.dump(self.db_dict[name], f)

    def shutdown(self):
        self.writeback_alldbs()

    def writeback_alldbs(self):
        # for name in self.db_dict:
        #     self.writeback_db(name)
        path = f'{ROOT_DIR}/alldbs{META_SUFFIX}'
        with open(path, 'wb') as f:
            pickle.dump(self.db_dict, f)

    def load_alldbs(self):
        path = f'{ROOT_DIR}/alldbs{META_SUFFIX}'
        if os.path.exists(path):
            with open(path, 'rb') as f:
                self.db_dict = pickle.load(f)

    def drop_db(self, name):
        if name not in self.db_dict:
            raise Exception(f'database {name} does not exist!')

        self.db_dict.pop(name)

    def use_db(self, name):
        if name not in self.db_dict:  # FIXME: 启动时没有恢复
            raise Exception(f'database {name} does not exist!')
        self.current_db = self.db_dict[name]

    def create_table(self, tablemeta):
        self.current_db.create_table(tablemeta)

    def drop_table(self, name):
        self.current_db.drop_table(name)

    def get_table(self, name):
        return self.current_db.get_table(name)
