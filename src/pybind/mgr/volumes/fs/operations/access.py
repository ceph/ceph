import errno
import json

def allow_access(mgr, client_entity, want_mds_cap, want_osd_cap,
               unwanted_mds_cap, unwanted_osd_cap):
    ret, out, err = mgr.mon_command({
        "prefix": "auth get",
        "entity": client_entity,
        "format": "json"})

    if ret == -errno.ENOENT:
        ret, out, err = mgr.mon_command({
            "prefix": "auth get-or-create",
            "entity": client_entity,
            "caps": ['mds',  want_mds_cap, 'osd', want_osd_cap, 'mon', 'allow r'],
            "format": "json"})
    else:
        cap = json.loads(out)[0]

        def cap_update(
                orig_mds_caps, orig_osd_caps, want_mds_cap,
                want_osd_cap, unwanted_mds_cap, unwanted_osd_cap):

            if not orig_mds_caps:
                return want_mds_cap, want_osd_cap

            mds_cap_tokens = orig_mds_caps.split(",")
            osd_cap_tokens = orig_osd_caps.split(",")

            if want_mds_cap in mds_cap_tokens:
                return orig_mds_caps, orig_osd_caps

            if unwanted_mds_cap in mds_cap_tokens:
                mds_cap_tokens.remove(unwanted_mds_cap)
                osd_cap_tokens.remove(unwanted_osd_cap)

            mds_cap_tokens.append(want_mds_cap)
            osd_cap_tokens.append(want_osd_cap)

            return ",".join(mds_cap_tokens), ",".join(osd_cap_tokens)

        orig_mds_caps = cap['caps'].get('mds', "")
        orig_osd_caps = cap['caps'].get('osd', "")

        mds_cap_str, osd_cap_str = cap_update(
            orig_mds_caps, orig_osd_caps, want_mds_cap, want_osd_cap,
            unwanted_mds_cap, unwanted_osd_cap)

        mgr.mon_command(
            {
                "prefix": "auth caps",
                'entity': client_entity,
                'caps': [
                    'mds', mds_cap_str,
                    'osd', osd_cap_str,
                    'mon', cap['caps'].get('mon', 'allow r')],
            })
        ret, out, err = mgr.mon_command(
            {
                'prefix': 'auth get',
                'entity': client_entity,
                'format': 'json'
            })

    # Result expected like this:
    # [
    #     {
    #         "entity": "client.foobar",
    #         "key": "AQBY0\/pViX\/wBBAAUpPs9swy7rey1qPhzmDVGQ==",
    #         "caps": {
    #             "mds": "allow *",
    #             "mon": "allow *"
    #         }
    #     }
    # ]

    caps = json.loads(out)
    assert len(caps) == 1
    assert caps[0]['entity'] == client_entity
    return caps[0]['key']

def deny_access(mgr, client_entity, want_mds_caps, want_osd_caps):
    ret, out, err = mgr.mon_command({
        "prefix": "auth get",
        "entity": client_entity,
        "format": "json",
    })

    if ret == -errno.ENOENT:
        # Already gone, great.
        return

    def cap_remove(orig_mds_caps, orig_osd_caps, want_mds_caps, want_osd_caps):
        mds_cap_tokens = orig_mds_caps.split(",")
        osd_cap_tokens = orig_osd_caps.split(",")

        for want_mds_cap, want_osd_cap in zip(want_mds_caps, want_osd_caps):
            if want_mds_cap in mds_cap_tokens:
                mds_cap_tokens.remove(want_mds_cap)
                osd_cap_tokens.remove(want_osd_cap)
                break

        return ",".join(mds_cap_tokens), ",".join(osd_cap_tokens)

    cap = json.loads(out)[0]
    orig_mds_caps = cap['caps'].get('mds', "")
    orig_osd_caps = cap['caps'].get('osd', "")
    mds_cap_str, osd_cap_str = cap_remove(orig_mds_caps, orig_osd_caps,
                                          want_mds_caps, want_osd_caps)

    if not mds_cap_str:
        mgr.mon_command(
            {
                'prefix': 'auth rm',
                'entity': client_entity
            })
    else:
        mgr.mon_command(
            {
                "prefix": "auth caps",
                'entity': client_entity,
                'caps': [
                    'mds', mds_cap_str,
                    'osd', osd_cap_str,
                    'mon', cap['caps'].get('mon', 'allow r')],
            })
