import logging
import os

from . import matrix

log = logging.getLogger(__name__)


def build_matrix(path, subset=None):
    """
    Return a list of items descibed by path such that if the list of
    items is chunked into mincyclicity pieces, each piece is still a
    good subset of the suite.

    A good subset of a product ensures that each facet member appears
    at least once.  A good subset of a sum ensures that the subset of
    each sub collection reflected in the subset is a good subset.

    A mincyclicity of 0 does not attempt to enforce the good subset
    property.

    The input is just a path.  The output is an array of (description,
    [file list]) tuples.

    For a normal file we generate a new item for the result list.

    For a directory, we (recursively) generate a new item for each
    file/dir.

    For a directory with a magic '+' file, we generate a single item
    that concatenates all files/subdirs (A Sum).

    For a directory with a magic '%' file, we generate a result set
    for each item in the directory, and then do a product to generate
    a result list with all combinations (A Product).

    For a directory with a magic '$' file, or for a directory whose name
    ends in '$', we generate a list of all items that we will randomly
    choose from.

    The final description (after recursion) for each item will look
    like a relative path.  If there was a % product, that path
    component will appear as a file with braces listing the selection
    of chosen subitems.

    :param path:        The path to search for yaml fragments
    :param subset:	(index, outof)
    """
    if subset:
        log.info(
            'Subset=%s/%s' %
            (str(subset[0]), str(subset[1]))
        )
    mat, first, matlimit = _get_matrix(path, subset)
    return generate_combinations(path, mat, first, matlimit)


def _get_matrix(path, subset=None):
    mat = None
    first = None
    matlimit = None
    if subset:
        (index, outof) = subset
        mat = _build_matrix(path, mincyclicity=outof)
        first = (mat.size() / outof) * index
        if index == outof or index == outof - 1:
            matlimit = mat.size()
        else:
            matlimit = (mat.size() / outof) * (index + 1)
    else:
        first = 0
        mat = _build_matrix(path)
        matlimit = mat.size()
    return mat, first, matlimit


def _build_matrix(path, mincyclicity=0, item=''):
    if not os.path.exists(path):
        raise IOError('%s does not exist (abs %s)' % (path, os.path.abspath(path)))
    if os.path.isfile(path):
        if path.endswith('.yaml'):
            return matrix.Base(item)
        return None
    if os.path.isdir(path):
        if path.endswith('.disable'):
            return None
        files = sorted(os.listdir(path))
        if len(files) == 0:
            return None
        if '+' in files:
            # concatenate items
            files.remove('+')
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity,
                    fn)
                if submat is not None:
                    submats.append(submat)
            return matrix.Concat(item, submats)
        elif path.endswith('$') or '$' in files:
            # pick a random item -- make sure we don't pick any magic files
            if '$' in files:
                files.remove('$')
            if '%' in files:
                files.remove('%')
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity,
                    fn)
                if submat is not None:
                    submats.append(submat)
            return matrix.PickRandom(item, submats)
        elif '%' in files:
            # convolve items
            files.remove('%')
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity=0,
                    item=fn)
                if submat is not None:
                    submats.append(submat)
            mat = matrix.Product(item, submats)
            if mat and mat.cyclicity() < mincyclicity:
                mat = matrix.Cycle(
                    (mincyclicity + mat.cyclicity() - 1) / mat.cyclicity(), mat
                )
            return mat
        else:
            # list items
            submats = []
            for fn in sorted(files):
                submat = _build_matrix(
                    os.path.join(path, fn),
                    mincyclicity,
                    fn)
                if submat is None:
                    continue
                if submat.cyclicity() < mincyclicity:
                    submat = matrix.Cycle(
                        ((mincyclicity + submat.cyclicity() - 1) /
                         submat.cyclicity()),
                        submat)
                submats.append(submat)
            return matrix.Sum(item, submats)
    assert False, "Invalid path %s seen in _build_matrix" % path
    return None


def generate_combinations(path, mat, generate_from, generate_to):
    """
    Return a list of items describe by path

    The input is just a path.  The output is an array of (description,
    [file list]) tuples.

    For a normal file we generate a new item for the result list.

    For a directory, we (recursively) generate a new item for each
    file/dir.

    For a directory with a magic '+' file, we generate a single item
    that concatenates all files/subdirs.

    For a directory with a magic '%' file, we generate a result set
    for each item in the directory, and then do a product to generate
    a result list with all combinations.

    The final description (after recursion) for each item will look
    like a relative path.  If there was a % product, that path
    component will appear as a file with braces listing the selection
    of chosen subitems.
    """
    ret = []
    for i in range(generate_from, generate_to):
        output = mat.index(i)
        ret.append((
            matrix.generate_desc(combine_path, output),
            matrix.generate_paths(path, output, combine_path)))
    return ret


def combine_path(left, right):
    """
    os.path.join(a, b) doesn't like it when b is None
    """
    if right:
        return os.path.join(left, right)
    return left
