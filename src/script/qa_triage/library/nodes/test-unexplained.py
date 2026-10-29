import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-06-04_09:53:35-rados-wip-bharath5-testing-2026-06-02-1032-distro-default-trial/263736': 'test/unexplained/ssh-connection-lost',
}


@node('test/unexplained', 'Why did the test fail?')
def test_unexplained(ctx):
    lost = ctx.search('teuthology.log',
                      r'tasks\.cephfs_test_runner:.*ConnectionLostError: SSH connection to (\S+) was lost',
                      'tasks.cephfs_test_runner:', 'ConnectionLostError: SSH connection to')
    if lost:
        host = re.search(r'SSH connection to (\S+) was lost', lost[0].text).group(1)
        return done('ssh-connection-lost',
                    f'teuthology lost its SSH connection to {host} during the test{at(lost[0])}',
                    lost[0],
                    key=f'SSH connection to {host} was lost',
                    host=host)
    return unknown('none of the failures this node knows')
