# from ..config import root_dir

class MetaManager:
    def __init__(self, fm, root_dir):
        self.fm = fm
        self.root_dir = root_dir
        self.db_dict = {}

    def drop_db(self, name):
        if name not in self.db_dict:
            raise Exception(f'database {name} does not exist!')

        self.db_dict.pop(name)


