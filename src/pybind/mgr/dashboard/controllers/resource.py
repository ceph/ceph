from weakref import WeakValueDictionary
from collections import OrderedDict
import six

from .attribute import Attribute, StaticAttribute

class ResourceMeta(type):
    ATTRS = '_attributes'
    KEY_ATTRS = '_key_attributes'

    def __new__(mcs, name, bases, attrs):
        sorted_attrs = tuple(sorted(
            ((k, v) for k, v in attrs.items() if isinstance(v, Attribute)),
            key=lambda i: i[1].count
        ))
        attrs[ResourceMeta.ATTRS] = tuple(k for k, v in sorted_attrs if not v.private)
        attrs[ResourceMeta.KEY_ATTRS] = tuple(k for k, v in sorted_attrs if v.key)
        if any(isinstance(v, StaticAttribute) for k, v in sorted_attrs):
            StaticAttribute.init_meta(attrs)

        return super(ResourceMeta, mcs).__new__(mcs, name, bases, attrs)


@six.add_metaclass(ResourceMeta)
class Resource(object):
    def __new__(cls, **kwargs):
        if not hasattr(cls, 'instances'):
            cls.instances = WeakValueDictionary()
        key_dict = {k:kwargs[k] for k in Resource.get_attrs(cls, key=True)}
        key = tuple(key_dict.items())

        try:
            instance = cls.instances[key]
        except KeyError:
            instance = super(Resource, cls).__new__(cls)
            instance.__init__(**key_dict)
            cls.instances[key] = instance
        return instance

    def __init__(self, **kwargs):
        for key in self.get_attrs(key=True):
            self[key] = kwargs[key]

    def _key(self):
        return tuple(self[k] for k in self.get_attrs(key=True))

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        return (type(self) is type(other)) and (self._key() == other._key())

    def get_attrs(self, key=False):
        return getattr(self, ResourceMeta.KEY_ATTRS if key else ResourceMeta.ATTRS)

    def __getitem__(self, key):
        if key in self.get_attrs():
            return getattr(self, key)
        else:
            raise KeyError

    def __setitem__(self, key, value):
        if key in self.get_attrs():
            return setattr(self, key, value)
        else:
            raise KeyError

    def __iter__(self):
        return iter(self.get_attrs())

    def to_dict(self, attrs=None):
        return OrderedDict(
            [(k, self[k]) for k in self if k in (attrs or self.get_attrs())])
