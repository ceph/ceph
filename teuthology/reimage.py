import argparse
import logging

import teuthology

from teuthology.parallel import parallel
from teuthology.provision import reimage, get_reimage_types
from teuthology.lock import query, ops
from teuthology.misc import get_user
from teuthology.misc import decanonicalize_hostname as shortname

log = logging.getLogger(__name__)

def main(args):
    if (args['--verbose']):
        teuthology.log.setLevel(logging.DEBUG)

    ctx = argparse.Namespace()
    ctx.os_type = args['--os-type']
    ctx.os_version = args['--os-version']

    nodes = args['<nodes>']

    reimage_types = get_reimage_types()
    statuses = query.get_statuses(nodes)
    owner = args['--owner'] or get_user()
    unlocked = [shortname(_['name'])
                            for _ in statuses if not _['locked']]
    if unlocked:
        log.error(
            "Some of the nodes are not locked: %s", unlocked)
        exit(1)

    improper = [shortname(_['name']) for _ in statuses if _['locked_by'] != owner]
    if improper:
        log.error(
            "Some of the nodes are not owned by '%s': %s", owner, improper)
        exit(1)

    irreimageable = [shortname(_['name']) for _ in statuses
                                if _['machine_type'] not in reimage_types]
    if irreimageable:
        log.error(
            "Following nodes cannot be reimaged because theirs machine type "
            "is not reimageable: %s", irreimageable)
        exit(1)

    def reimage_node(ctx, machine_name, machine_type):
        ops.update_nodes([machine_name], True)
        reimage(ctx, machine_name, machine_type)
        ops.update_nodes([machine_name])
        log.debug("Node '%s' reimaging is complete", machine_name)

    with parallel() as p:
        for node in statuses:
            log.debug("Start node '%s' reimaging", node['name'])
            p.spawn(reimage_node, ctx, shortname(node['name']), node['machine_type'])
