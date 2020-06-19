import argparse
import os

from logging import debug
from teuthology import misc
from teuthology.orchestra import cluster
from teuthology.orchestra.run import quote
from teuthology.orchestra.daemon.group import DaemonGroup
import subprocess


class FakeRemote(object):
    pass


def test_pid():
    ctx = argparse.Namespace()
    ctx.daemons = DaemonGroup(use_systemd=True)
    remote = FakeRemote()

    ps_ef_output_path = os.path.join(
        os.path.dirname(__file__),
        "files/daemon-systemdstate-pid-ps-ef.output"
    )

    # patching ps -ef command output using a file
    def sh(args):
        args[0:2] = ["cat", ps_ef_output_path]
        debug(args)
        return subprocess.getoutput(quote(args))

    remote.sh = sh
    remote.init_system = 'systemd'
    remote.shortname = 'host1'

    ctx.cluster = cluster.Cluster(
        remotes=[
            (remote, ['rgw.0', 'mon.a', 'mgr.a', 'mds.a', 'osd.0'])
        ],
    )

    for remote, roles in ctx.cluster.remotes.items():
        for role in roles:
            _, rol, id_ = misc.split_role(role)
            if any(rol.startswith(x) for x in ['mon', 'mgr', 'mds']):
                ctx.daemons.register_daemon(remote, rol, remote.shortname)
            else:
                ctx.daemons.register_daemon(remote, rol, id_)

    for _, daemons in ctx.daemons.daemons.items():
        for daemon in daemons.values():
            pid = daemon.pid
            debug(pid)
            assert pid
