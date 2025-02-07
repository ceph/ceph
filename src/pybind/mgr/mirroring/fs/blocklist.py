import logging

log = logging.getLogger(__name__)

def blocklist(mgr, addr):
    cmd = {'prefix': 'osd blocklist', 'blocklistop': 'add', 'addr': str(addr)}
    r, outs, err = mgr.mon_command(cmd)
    if r != 0:
        log.error(f'blocklist error: {err}')
    return r
