
def make_fake_fstools(fake_filesystem):
    """
    Build a fake listdir() and isfile(), to be used instead of
    os.listir() and os.isfile()

    An example fake_filesystem value:
        >>> fake_fs = {
            'a_directory': {
                'another_directory': {
                    'a_file': None,
                    'another_file': None,
                },
                'random_file': None,
                'yet_another_directory': {
                    'empty_directory': {},
                },
            },
        }

        >>> fake_listdir = make_fake_listdir(fake_fs)
        >>> fake_listdir('a_directory/yet_another_directory')
        ['empty_directory']
        >>> fake_isfile('a_directory/yet_another_directory')
        False

    :param fake_filesystem: A dict representing a filesystem layout
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
                raise OSError(
                    '[Errno 2] No such file or directory: %s' % next_dir)
            subdict = subdict.get(next_dir)
            if not isinstance(subdict, dict):
                raise OSError('[Errno 20] Not a directory: %s' % next_dir)
            if subdict and not remainder:
                return subdict.keys()
        return []

    def fake_isfile(path, fsdict=False):
        if fsdict is False:
            fsdict = fake_filesystem

        components = path.strip('/').split('/')
        subdict = fsdict
        for component in components:
            if component not in subdict:
                raise OSError(
                    '[Errno 2] No such file or directory: %s' % component)
            subdict = subdict.get(component)
        if subdict is None:
            return True
        else:
            return False

    def fake_isdir(path, fsdict=False):
        return not fake_isfile(path)
    return fake_listdir, fake_isfile, fake_isdir
