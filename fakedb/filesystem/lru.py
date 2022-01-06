from collections import OrderedDict


class LRU:
    def __init__(self, size):
        self.cache = OrderedDict()
        self.unused = set(list(range(size)))

    def assign(self):
        # return new_idx, need_write_back
        if len(self.unused) == 0:
            idx = self.cache.popitem(last=False)[0]
            self.unused.add(idx)
            return idx, True
        else:
            # idx = self.unused.pop()
            idx = next(iter(self.unused))
            return idx, False

    def free(self, idx):
        # assert idx not in self.unused
        self.cache.pop(idx)
        self.unused.add(idx)

    def access(self, idx):
        # assert idx not in self.unused
        #  self.cache[idx] = None
        if idx in self.cache:
            self.cache.move_to_end(idx, last=True)
        else:
            self.cache[idx] = None
            self.unused.remove(idx)
        