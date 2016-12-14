from teuthology.misc import get_testdir
from teuthology.orchestra import run


def write_secret_file(ctx, remote, role, keyring, filename):
    """
    Stash the kerying in the filename specified.
    """
    testdir = get_testdir(ctx)
    remote.run(
        args=[
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            'ceph-authtool',
            '--name={role}'.format(role=role),
            '--print-key',
            keyring,
            run.Raw('>'),
            filename,
            ],
        )
