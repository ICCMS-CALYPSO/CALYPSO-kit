import itertools

if "pairwise" not in dir(itertools):

    def pairwise(iterable):
        # pairwise('ABCDEFG') --> AB BC CD DE EF FG
        a, b = itertools.tee(iterable)
        next(b, None)
        return zip(a, b)

else:
    pairwise = itertools.pairwise


class groupby_delta:
    """groupby delta

    >>> l = [1.0, 1.04, 1.1, 3.1, 3.14, 3.4]
    >>> for i in groupby_delta(l, 0.1):
    >>>     print(i)
    (1.0, 1.04, 1.1)
    (3.1, 3.14)
    (3.4,)
    """

    def __init__(self, iter, delta):
        self.it = itertools.pairwise(iter)
        self.delta = delta

    def __iter__(self):
        return self

    def __next__(self):
        group = []
        self.final_left = []
        for a, b in self.it:
            if len(group) == 0:
                group.append(a)
                self.final_left = []
            if b - a < self.delta:
                group.append(b)
            else:
                self.final_left.append(b)
                return tuple(group)
        else:
            if len(group) != 0:
                return tuple(group)
            if len(self.final_left) != 0:
                return (self.final_left.pop(),)
            raise StopIteration()
