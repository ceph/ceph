import errno
import os

def munge(path):
    """
    Munge a potentially hostile path name to be safe to use.

    This very definitely changes the meaning of the path,
    but it only does that for unsafe paths.
    """
    # explicitly ignoring windows as a platform
    segments = path.split('/')
    # filter out empty segments like foo//bar
    segments = [s for s in segments if s!='']
    # filter out no-op segments like foo/./bar
    segments = [s for s in segments if s!='.']
    # all leading dots become underscores; makes .. safe too
    for idx, seg in enumerate(segments):
        if seg.startswith('.'):
            segments[idx] = '_'+seg[1:]
    # empty string, "/", "//", etc
    if not segments:
        segments = ['_']
    return '/'.join(segments)


def makedirs(root, path):
    """
    os.makedirs gets confused if the path contains '..', and root might.

    This relies on the fact that `path` has been normalized by munge().
    """
    segments = path.split('/')
    for seg in segments:
        root = os.path.join(root, seg)
        try:
            os.mkdir(root)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise
