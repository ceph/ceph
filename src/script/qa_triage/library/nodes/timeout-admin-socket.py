from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

EXAMPLES = {
    'skanta-2026-10-12_23:32:34-rados-wip-bharath20-testing-20261012.132505-main-distro-default-trial/631182': 'timeout/admin-socket/osd-init-deadlock',
}


@node('timeout/admin-socket', 'Why did the admin socket time out on a restarting OSD?')
def timeout_admin_socket(ctx):
    config_set = ctx.search(
        'teuthology.log',
        r'ceph daemon osd\.\S+ config set',
        'ceph daemon osd.',
        'config set',
    )
    if not config_set:
        return unknown('no ceph daemon osd config set command found near the admin socket timeout')
    fbmap_shutdown = ctx.search(
        'remote/*/log/ceph-osd.*.log.gz',
        r'fbmap_alloc \S+ shutdown',
        'fbmap_alloc',
        'shutdown',
    )
    if not fbmap_shutdown:
        return unknown('no fbmap_alloc shutdown in OSD log')
    return done(
        'osd-init-deadlock',
        f'OSD::init() held osd_lock during BlueStore mount: a "ceph daemon osd config set" '
        f'command{at(config_set[0])} tried to route through OSD::handle_conf_change() which '
        f'also needs osd_lock, so the admin socket timed out and the OSD never finished booting '
        f'(OSD log shows fbmap_alloc shutdown{at(fbmap_shutdown[0])})',
        config_set[0],
        fbmap_shutdown[0],
        key='osd init deadlock: admin socket config set during BlueStore mount',
        wait=ctx.facts.get('wait', ''),
    )
