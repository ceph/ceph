from io import BytesIO
from contextlib import closing


try:
     FileNotFoundError, NotADirectoryError
except NameError:
     FileNotFoundError = NotADirectoryError = OSError


def make_fake_fstools(fake_filesystem):
    """
    Build fake versions of os.listdir(), os.isfile(), etc. for use in
    unit tests

    An example fake_filesystem value:
        >>> fake_fs = {\
            'a_directory': {\
                'another_directory': {\
                    'empty_file': None,\
                    'another_empty_file': None,\
                },\
                'random_file': None,\
                'yet_another_directory': {\
                    'empty_directory': {},\
                },\
                'file_with_contents': 'data',\
            },\
        }
        >>> fake_listdir, fake_isfile, _, _ = \
                make_fake_fstools(fake_fs)
        >>> fake_listdir('a_directory/yet_another_directory')
        ['empty_directory']
        >>> fake_isfile('a_directory/yet_another_directory')
        False

    :param fake_filesystem: A dict representing a filesystem
    """
    assert isinstance(fake_filesystem, dict)

    def fake_listdir(path, fsdict=False):
        if fsdict is False:
            fsdict = fake_filesystem

        remainder = path.strip('/') + '/'
        subdict = fsdict
        while '/' in remainder:
            next_dir, remainder = remainder.split('/', 1)
            if next_dir not in subdict:
                raise FileNotFoundError(
                    '[Errno 2] No such file or directory: %s' % next_dir)
            subdict = subdict.get(next_dir)
            if not isinstance(subdict, dict):
                raise NotADirectoryError('[Errno 20] Not a directory: %s' % next_dir)
            if subdict and not remainder:
                return list(subdict)
        return []

    def fake_isfile(path, fsdict=False):
        if fsdict is False:
            fsdict = fake_filesystem

        components = path.strip('/').split('/')
        subdict = fsdict
        for component in components:
            if component not in subdict:
                raise FileNotFoundError(
                    '[Errno 2] No such file or directory: %s' % component)
            subdict = subdict.get(component)
        return subdict is None or isinstance(subdict, str)

    def fake_isdir(path, fsdict=False):
        return not fake_isfile(path)

    def fake_exists(path, fsdict=False):
        return fake_isfile(path, fsdict) or fake_isdir(path, fsdict)

    def fake_open(path, mode=None, buffering=None):
        components = path.strip('/').split('/')
        subdict = fake_filesystem
        for component in components:
            if component not in subdict:
                raise IOError(
                    '[Errno 2] No such file or directory: %s' % component)
            subdict = subdict.get(component)
        if isinstance(subdict, dict):
            raise IOError('[Errno 21] Is a directory: %s' % path)
        elif subdict is None:
            return closing(BytesIO(b''))
        return closing(BytesIO(subdict.encode()))

    return fake_exists, fake_listdir, fake_isfile, fake_isdir, fake_open
