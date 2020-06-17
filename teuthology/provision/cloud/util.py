import datetime
import dateutil.tz
import dateutil.parser
import json
import os

from teuthology.util.flock import FileLock

def get_user_ssh_pubkey(path='~/.ssh/id_rsa.pub'):
    full_path = os.path.expanduser(path)
    if not os.path.exists(full_path):
        return
    with open(full_path) as f:
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

    Example::

        >>> a = dict(x=0, y=1, z=3)
        >>> b = dict(x=1, y=2, z=0)
        >>> selective_update(a, b, lambda foo, bar: foo > bar)
        >>> print(a)
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
        elif func(value, a[key]):
            a[key] = value


class AuthToken(object):
    time_format = '%Y-%m-%d %H:%M:%S%z'

    def __init__(self, name, directory=os.path.expanduser('~/.cache/')):
        self.name = name
        self.directory = directory
        self.path = os.path.join(directory, name)
        self.lock_path = "%s.lock" % self.path
        self.expires = None
        self.value = None
        self.endpoint = None

    def read(self):
        if not os.path.exists(self.path):
            self.value = None
            self.expires = None
            self.endpoint = None
            return
        with open(self.path, 'r') as obj:
            string = obj.read()
        obj = json.loads(string)
        self.expires = dateutil.parser.parse(obj['expires'])
        if self.expired:
            self.value = None
            self.endpoint = None
        else:
            self.value = obj['value']
            self.endpoint = obj['endpoint']

    def write(self, value, expires, endpoint):
        obj = dict(
            value=value,
            expires=datetime.datetime.strftime(expires, self.time_format),
            endpoint=endpoint,
        )
        string = json.dumps(obj)
        with open(self.path, 'w') as obj:
            obj.write(string)

    @property
    def expired(self):
        if self.expires is None:
            return True
        utcnow = datetime.datetime.now(dateutil.tz.tzutc())
        offset = datetime.timedelta(minutes=30)
        return self.expires < (utcnow + offset)

    def __enter__(self):
        with FileLock(self.lock_path):
            self.read()
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
