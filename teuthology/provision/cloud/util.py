import os


def get_user_ssh_pubkey(path='~/.ssh/id_rsa.pub'):
    full_path = os.path.expanduser(path)
    if not os.path.exists(full_path):
        return
    with file(full_path, 'rb') as f:
        return f.read().strip()


def combine_dicts(list_of_dicts, func):
    """
    A useful function to merge a list of dicts. Most of the work is done by
    selective_update().

    :param list_of_dicts: A list of dicts to combine using selective_update()
    :param func:          A comparison function that will be passed to
                          selective_update() along with values from each input
                          dict
    :returns:             The new, merged, dict
    """
    new_dict = dict()
    for item in list_of_dicts:
        selective_update(new_dict, item, func)
    return new_dict


def selective_update(a, b, func):
    """
    Given two dicts and a comparison function, recursively inspects key-value
    pairs in the second dict and merges them into the first dict if func()
    returns a "Truthy" value.

    Example:

        >>> a = dict(x=0, y=1, z=3)
        >>> b = dict(x=1, y=2, z=0)
        >>> selective_update(a, b, lambda foo, bar: foo > bar)
        >>> print a
        {'x': 1, 'y': 2, 'z': 3}

    :param a:    A dict. This is modified in-place!
    :param b:    Another dict.
    :param func: A binary comparison function that will be called similarly to:
                 func(a[key], b[key]) for each key in b.
    """
    for key, value in b.items():
        if key not in a:
            a[key] = value
            continue
        if isinstance(value, dict):
            selective_update(a[key], value, func)
        if func(value, a[key]):
            a[key] = value
