import os
import heapq
from fractions import gcd

def lcm(a, b):
    return a*b // gcd(a, b)
def lcml(l):
    return reduce(lcm, l)

class Matrix:
    """
    Interface for sets
    """
    def size(self):
        pass

    def index(self, i):
        """
        index() should return a recursive structure represending the paths
        to concatenate for index i:

        Result :: (PathSegment, Result) | {Result}
        Path :: string

        {Result} is a frozen_set of Results indicating that
        the set of paths resulting from each of the contained
        Results should be concatenated.  (PathSegment, Result)
        indicates that PathSegment should be prepended to the
        paths resulting from Result.
        """
        pass

    def minscanlen(self):
        """
        min run require to get a good sample
        """
        pass

    def cyclicity(self):
        """
        A cyclicity of N means that the set represented by the Matrix
        can be chopped into N good subsets of sequential indices.
        """
        return self.size() / self.minscanlen()

    def tostr(self, depth):
        pass

    def __str__(self):
        """
        str method
        """
        return self.tostr(0)


class Cycle(Matrix):
    """
    Run a matrix multiple times
    """
    def __init__(self, num, mat):
        self.mat = mat
        self.num = num

    def size(self):
        return self.mat.size() * self.num

    def index(self, i):
        return self.mat.index(i % self.mat.size())

    def minscanlen(self):
        return self.mat.minscanlen()

    def tostr(self, depth):
        return '\t'*depth + "Cycle({num}):\n".format(num=self.num) + self.mat.tostr(depth + 1)

class Base(Matrix):
    """
    Just a single item.
    """
    def __init__(self, item):
        self.item = item

    def size(self):
        return 1

    def index(self, i):
        return self.item

    def minscanlen(self):
        return 1

    def tostr(self, depth):
        return '\t'*depth + "Base({item})\n".format(item=self.item)


class Product(Matrix):
    """
    Builds items by taking one item from each submatrix.  Contiguous
    subsequences should move through all dimensions.
    """
    def __init__(self, item, _submats):
        assert len(_submats) > 0, \
            "Product requires child submats to be passed in"
        self.item = item

        submats = sorted(
            [((i.size(), ind), i) for (i, ind) in
             zip(_submats, range(len(_submats)))], reverse=True)
        self.submats = []
        self._size = 1
        for ((size, _), submat) in submats:
            self.submats.append((self._size, submat))
            self._size *= size
        self.submats.reverse()

        self._minscanlen = max([i.minscanlen() for i in _submats])
        if self._minscanlen + 1 > self._size:
            self._minscanlen  = self._size
        else:
            self._minscanlen += 1

    def tostr(self, depth):
        ret = '\t'*depth + "Product({item}):\n".format(item=self.item)
        return ret + ''.join([i[1].tostr(depth+1) for i in self.submats])

    def minscanlen(self):
        return self._minscanlen

    def size(self):
        return self._size

    def _index(self, i, submats):
        """
        We recursively reduce the N dimension problem to a two
        dimension problem.

        index(i) = (lmat.index(i % lmat.size()), rmat.index(i %
        rmat.size())) would simply work if lmat.size() and rmat.size()
        are relatively prime.

        In general, if the gcd(lmat.size(), rmat.size()) == N,
        index(i) would be periodic on the interval (lmat.size() *
        rmat.size()) / N.  To adjust, we decrement the lmat index
        number on each repeat.  Each of the N repeats must therefore
        be distinct from the previous ones resulting in lmat.size() *
        rmat.size() combinations.
        """
        assert len(submats) > 0, \
            "_index requires non-empty submats"
        if len(submats) == 1:
            return frozenset([submats[0][1].index(i)])

        lmat = submats[0][1]
        lsize = lmat.size()

        rsize = submats[0][0]

        cycles = gcd(rsize, lsize)
        clen = (rsize * lsize) / cycles
        off = (i / clen) % cycles

        def combine(r, s=frozenset()):
            if type(r) is frozenset:
                return s | r
            return s | frozenset([r])

        litems = lmat.index((i - off) % lmat.size())
        ritems = self._index(i, submats[1:])
        return combine(litems, combine(ritems))

    def index(self, i):
        items = self._index(i, self.submats)
        return (self.item, items)

class Concat(Matrix):
    """
    Concatenates all items in child matrices
    """
    def __init__(self, item, submats):
        self.submats = submats
        self.item = item

    def size(self):
        return 1

    def minscanlen(self):
        return 1

    def index(self, i):
        out = frozenset()
        for submat in self.submats:
            for i in range(submat.size()):
                out = out | frozenset([submat.index(i)])
        return (self.item, out)

    def tostr(self, depth):
        ret = '\t'*depth + "Concat({item}):\n".format(item=self.item)
        return ret + ''.join([i[1].tostr(depth+1) for i in self.submats])

class Sum(Matrix):
    """
    We want to mix the subsequences proportionately to their size.

    The intuition is that we map all of the subsequences uniformly
    onto rational numbers in [0, 1).  The ith subsequence with length
    l will have index k map onto i*<epsilon> + k*(1/l).  i*<epsilon>
    ensures that no two subsequences have an index which shares a
    mapping in [0, 1) as long as <epsilon> is chosen to be small
    enough.

    Rather than actually dealing with rational numbers, however, we'll
    instead map onto whole numbers in [0, pseudo_size) where
    pseudo_size is the lcm of the subsequence lengths * the number of
    subsequences.  Including the number of subsequences in the product
    allows us to use 1 as <epsilon>.  For each subsequence, we designate
    an offset (position in input list) and a multiple (pseudo_size / size)
    such that the psuedo_index for index i is <offset> + i*<multiple>.

    I don't have a good way to map index to pseudo index, so we'll
    precompute a mapping in the constructor (self._i_so_sis) from
    index to (subset_index, subset).
    """
    def __init__(self, item, _submats):
        assert len(_submats) > 0, \
            "Sum requires non-empty _submats"
        self.item = item

        self._pseudo_size = lcml((i.size() for i in _submats)) * len(_submats)
        self._size = sum((i.size() for i in _submats))
        self._submats = [
            ((i, self._pseudo_size / s.size()), s) for (i, s) in \
            zip(range(len(_submats)), _submats)
        ]

        def sm_to_pmsl(((offset, multiple), submat)):
            """
            submat tuple to pseudo minscanlen
            """
            return submat.minscanlen() * multiple

        def index_to_pindex_generator(submats):
            assert len(submats) > 0, "submats must be non-empty"
            h = []
            for (offset, multiple), submat in submats:
                heapq.heappush(h, (offset, 0, multiple, submat))
            while True:
                cur, si, multiple, submat = heapq.heappop(h)
                heapq.heappush(
                    h,
                    (cur + multiple, si + 1, multiple, submat))
                yield si, submat

        self._i_to_sis = dict(
            zip(range(self._size), index_to_pindex_generator(self._submats))
        )

        self._minscanlen = self.pseudo_index_to_index(
            max(map(sm_to_pmsl, self._submats)))

    def pi_to_sis(self, pi, (offset, multiple)):
        """
        max(i) s.t. offset + i*multiple <= pi
        """
        if pi < offset:
            return -1
        return (pi - offset) / multiple

    def pseudo_index_to_index(self, pi):
        """
        Count all pseudoindex values <= pi with corresponding subset indices
        """
        return sum((self.pi_to_sis(pi, i) + 1 for i, _ in self._submats)) - 1

    def tostr(self, depth):
        ret = '\t'*depth + "Sum({item}):\n".format(item=self.item)
        return ret + ''.join([i[1].tostr(depth+1) for i in self._submats])

    def minscanlen(self):
        return self._minscanlen

    def size(self):
        return self._size

    def index(self, i):
        si, submat = self._i_to_sis[i % self._size]
        return (self.item, submat.index(si))

def generate_lists(result):
    """
    Generates a set of tuples representing paths to concatenate
    """
    if type(result) is frozenset:
        ret = []
        for i in result:
            ret.extend(generate_lists(i))
        return frozenset(ret)
    elif type(result) is tuple:
        ret = []
        (item, children) = result
        for f in generate_lists(children):
            nf = [item]
            nf.extend(f)
            ret.append(tuple(nf))
        return frozenset(ret)
    else:
        return frozenset([(result,)])


def generate_paths(path, result, joinf=os.path.join):
    """
    Generates from the result set a list of sorted paths to concatenate
    """
    return [reduce(joinf, i, path) for i in sorted(generate_lists(result))]


def generate_desc(joinf, result):
    """
    Generates the text description of the test represented by result
    """
    if type(result) is frozenset:
        ret = []
        for i in sorted(result):
            ret.append(generate_desc(joinf, i))
        return '{' + ' '.join(ret) + '}'
    elif type(result) is tuple:
        (item, children) = result
        cdesc = generate_desc(joinf, children)
        return joinf(str(item), cdesc)
    else:
        return str(result)
