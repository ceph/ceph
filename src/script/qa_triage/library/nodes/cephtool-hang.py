from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-10-03_22:27:10-rados-wip-bharath5-testing-20261003.131043-main-distro-default-trial/605162': 'workunit/unexplained/cephtool-hang/bad-caps-key-left-behind',
}


@node('workunit/unexplained/cephtool-hang', 'Why did the mon keep logging invalid auth capabilities?')
def cephtool_hang(ctx):
    made = ctx.search('teuthology.log',
                      r"test_mon_caps:\s+ceph-authtool -n client\.bug --cap mon ''",
                      "--cap mon ''")
    health = ctx.search('teuthology.log',
                        r'Health check failed: \d+ auth entities have invalid capabilities \(AUTH_BAD_CAPS\)',
                        'AUTH_BAD_CAPS')
    if made and health:
        return done('bad-caps-key-left-behind',
                    f'test_mon_caps added client.bug with an empty mon caps string{at(made[0])} '
                    'and never deleted it, so the mon kept the AUTH_BAD_CAPS health error '
                    f'(first raised{at(health[0])}) and the later ceph tell commands of the '
                    'workunit hung until its timeout',
                    made[0], health[0],
                    key='cephtool test_mon_caps left client.bug with empty mon caps (AUTH_BAD_CAPS)')
    return unknown('none of the causes this node knows')
