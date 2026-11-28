import smb


def test_entry_cache_contents_only_lists_the_namespace():
    store = smb.config_store.MemConfigStore()
    store[('ns1', 'a')].set({'v': 1})
    store[('ns1', 'b')].set({'v': 2})
    store[('ns2', 'c')].set({'v': 3})

    cache = smb.config_store.EntryCache()
    for key in [('ns1', 'a'), ('ns1', 'b'), ('ns2', 'c')]:
        cache[key] = store[key]

    assert sorted(cache.contents('ns1')) == ['a', 'b']
    assert sorted(cache.contents('ns2')) == ['c']
    assert list(cache.contents('missing')) == []
