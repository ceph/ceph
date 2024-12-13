import errno
import json
from typing import List


def prepare_updated_caps_list(existing_caps, mds_cap_str, osd_cap_str, authorize=True):
    caps_list: List[str]  = []
    for k, v in existing_caps['caps'].items():
        if k == 'mds' or k == 'osd':
            continue
        elif k == 'mon':
            if not authorize and v == 'allow r':
                continue
        caps_list.extend((k, v))

    if mds_cap_str:
        caps_list.extend(('mds', mds_cap_str))
    if osd_cap_str:
        caps_list.extend(('osd', osd_cap_str))

    if authorize and 'mon' not in caps_list:
        caps_list.extend(('mon', 'allow r'))

    return caps_list


def allow_access(mgr, client_entity, want_mds_cap, want_osd_cap,
                 unwanted_mds_cap, unwanted_osd_cap, existing_caps):
    if existing_caps is None:
        ret, out, err = mgr.mon_command({
            "prefix": "auth get-or-create",
            "entity": client_entity,
            "caps": ['mds', want_mds_cap, 'osd', want_osd_cap, 'mon', 'allow r'],
            "format": "json"})
    else:
        cap = existing_caps[0]

        def cap_update(
                orig_mds_caps, orig_osd_caps, want_mds_cap,
                want_osd_cap, unwanted_mds_cap, unwanted_osd_cap):

            if not orig_mds_caps:
                return want_mds_cap, want_osd_cap

            mds_cap_tokens = [x.strip() for x in orig_mds_caps.split(",")]
            osd_cap_tokens = [x.strip() for x in orig_osd_caps.split(",")]

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

        caps_list = prepare_updated_caps_list(cap, mds_cap_str, osd_cap_str)
        mgr.mon_command(
            {
                "prefix": "auth caps",
                'entity': client_entity,
                'caps': caps_list
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
        mds_cap_tokens = [x.strip() for x in orig_mds_caps.split(",")]
        osd_cap_tokens = [x.strip() for x in orig_osd_caps.split(",")]

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

    caps_list = prepare_updated_caps_list(cap, mds_cap_str, osd_cap_str, authorize=False)
    if not caps_list:
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
                'caps': caps_list
            })
