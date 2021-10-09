from collections import OrderedDict


class LRU:
    def __init__(self, size):
        self.cache = OrderedDict()
        self.unused = set(list(range(size)))

    def assign(self):
        if len(self.unused) == 0:
            idx = self.cache.popitem(last=False)
            return idx
        else:
            idx = self.unused.pop()
            self.cache[idx] = None
            return idx

    def free(self, idx):
        # assert idx not in self.unused
        self.cache.pop(idx)
        self.unused.add(idx)

    def access(self, idx):
        # assert idx not in self.unused
        self.cache[idx] = None
