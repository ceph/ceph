# -*- coding: utf-8 -*-
from __future__ import absolute_import

try:
    from typing import Dict, Any  # pylint: disable=unused-import
except ImportError:
    pass


def update_dict(data, update_data):
    # type: (Dict[Any, Any], Dict[Any, Any]) -> Dict[Any]
    """ Update a dictionary recursively.

    Eases doing so by providing the option to separate the key to be updated by dot characters.  If
    a key provided does not exist, it will raise an KeyError instead of just updating the
    dictionary.

    Limitations

    Please note that the functionality provided by this method can only be used if the dictionary to
    be updated (`data`) does not contain dot characters in its keys.

    :raises KeyError:

    >>> update_dict({'foo': {'bar': 5}}, {'foo.bar': 10})
    {'foo': {'bar': 10}}

    >>> update_dict({'foo': {'bar': 5}}, {'xyz': 10})
    Traceback (most recent call last):
        ...
    KeyError: 'xyz'

    >>> update_dict({'foo': {'bar': 5}}, {'foo.xyz': 10})
    Traceback (most recent call last):
        ...
    KeyError: 'xyz'
    """
    for k, v in update_data.items():
        keys = k.split('.')
        element = None
        for i, key in enumerate(keys):
            last = False
            if len(keys) == i + 1:
                last = True

            if not element:
                element = data[key]
            elif not last:
                element = element[key]  # pylint: disable=unsubscriptable-object

            if last:
                if key not in element:
                    raise KeyError(key)

                element[key] = v
    return data
