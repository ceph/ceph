from threading import RLock
from ..helpers.ttl_cache import ttl_cache

class Attribute(object):
    count = 0
    lock = RLock()

    def __init__(
            self,
            fget,
            fset=None,
            key=False,
            version=None,
            private=False,
            cache_ttl=None,
            fget_exc_pass=None,
    ):
        with Attribute.lock:
            self.count = Attribute.count
            Attribute.count += 1

        if fget and cache_ttl:
            self.fget = ttl_cache(ttl=cache_ttl,)(fget)
        else:
            self.fget = fget

        self.fset = fset

        self.key = key
        self.cache_ttl = cache_ttl
        self.version = version
        self.private = private

    def __get__(self, obj, objtype=None):
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        self.fset(obj, value)

class PrivateAttribute(Attribute):
    def __init__(self, fget, **kwargs):
        kwargs.update(dict(private=True))
        super(PrivateAttribute, self).__init__(fget, **kwargs)

class StaticAttribute(Attribute):
    STATIC = '_static'

    @classmethod
    def init_meta(cls, attrs):
        attrs[cls.STATIC] = {}

    def _key(self, obj):
        return (id(self), id(obj))

    def __init__(self, **kwargs):
        kwargs.update(dict(cache_ttl=None))
        super(StaticAttribute, self).__init__(
            lambda o: getattr(o, self.STATIC)[self._key(o)],
            fset=lambda o, v: getattr(o, self.STATIC).__setitem__(self._key(o), v),
            **kwargs)
