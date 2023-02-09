
import unittest

from ..plugins.ttl_cache import CacheManager, TTLCache


class TTLCacheTest(unittest.TestCase):
    def test_get(self):
        ref = 'testcache'
        cache = TTLCache(ref, 30)
        with self.assertRaises(KeyError):
            val = cache['foo']
        cache['foo'] = 'var'
        val = cache['foo']
        self.assertEqual(val, 'var')
        self.assertEqual(cache.hits, 1)
        self.assertEqual(cache.misses, 1)

    def test_ttl(self):
        ref = 'testcache'
        cache = TTLCache(ref, 0.0000001)
        cache['foo'] = 'var'
        # pylint: disable=pointless-statement
        with self.assertRaises(KeyError):
            cache['foo']
        self.assertEqual(cache.hits, 0)
        self.assertEqual(cache.misses, 1)
        self.assertEqual(cache.expired, 1)

    def test_maxsize_fifo(self):
        ref = 'testcache'
        cache = TTLCache(ref, 30, 2)
        cache['foo0'] = 'var0'
        cache['foo1'] = 'var1'
        cache['foo2'] = 'var2'
        # pylint: disable=pointless-statement
        with self.assertRaises(KeyError):
            cache['foo0']
        self.assertEqual(cache.hits, 0)
        self.assertEqual(cache.misses, 1)


class TTLCacheManagerTest(unittest.TestCase):
    def test_get(self):
        ref = 'testcache'
        cache0 = CacheManager.get(ref)
        cache1 = CacheManager.get(ref)
        self.assertEqual(id(cache0), id(cache1))
