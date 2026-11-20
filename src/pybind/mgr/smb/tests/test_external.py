import smb.config_store
import smb.external


def test_stored_cluster_ids_includes_alt_stores():
    public = smb.config_store.MemConfigStore()
    priv = smb.config_store.MemConfigStore()
    public.overwrite({('cfg1', 'config.smb'): {}})
    priv.overwrite(
        {
            ('cfg1', 'join.auth1.json'): {},
            ('cfg2', 'join.auth2.json'): {},
        }
    )

    assert set(smb.external.stored_cluster_ids(public)) == {'cfg1'}
    assert set(smb.external.stored_cluster_ids(public, priv)) == {
        'cfg1',
        'cfg2',
    }
