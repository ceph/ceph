import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'jayaprakash-2026-04-29_15:21:37-rados-wip-jaya-bs-testing-tentacle-29-04-2026-distro-default-trial/185866': 'cluster-log/other/cephadm-pull-manifest-unknown',
    'skanta-2026-04-06_14:05:55-rados-wip-bharath14-testing-2026-04-06-1412-squid-distro-default-trial/133531': 'cluster-log/other/cephadm-pull-manifest-unknown',
    'skanta-2026-03-07_15:39:05-rados-wip-bharath4-testing-2026-03-05-1456-tentacle-distro-default-trial/93028': 'cluster-log/other/cephadm-pull-manifest-not-found',
}


@node('cluster-log/other', 'What did the cluster log report?')
def cluster_log_other(ctx):
    if 'No such file or directory' in ctx.job.reason:
        # podman format: "reading manifest <tag> in <repo>: manifest unknown"
        # or quay format: "reading manifest <tag> in <repo>: unknown: Tag <tag> was deleted or has expired"
        manifest = ctx.search('teuthology.log',
                              r'reading manifest \S+ in \S+: (manifest unknown|unknown: Tag \S+ was deleted)',
                              'reading manifest', 'quay.ceph.io/ceph-ci/ceph', max_hits=1)
        if manifest:
            pull = ctx.search('teuthology.log',
                              r"Command failed on \S+ with status 1: 'sudo \S+/cephadm --image \S+ pull'",
                              'cephadm', 'pull', max_hits=1)
            evidence = [manifest[0]]
            if pull:
                evidence.append(pull[0])
            return done('cephadm-pull-manifest-unknown',
                        f'cephadm could not pull the container image: manifest unknown{at(manifest[0])}; '
                        'because no cluster was deployed, the teardown cluster log check failed with "No such file or directory" (tracker 76028)',
                        *evidence,
                        key='cephadm pull: reading manifest in quay.ceph.io/ceph-ci/ceph: manifest unknown')
        # docker format: "manifest for <repo>:<tag> not found: manifest unknown"
        manifest2 = ctx.search('teuthology.log',
                               r'manifest for \S+ not found: manifest unknown',
                               'manifest unknown', max_hits=1)
        if manifest2:
            pull = ctx.search('teuthology.log',
                              r"Command failed on \S+ with status 1: 'sudo \S+/cephadm --image \S+ pull'",
                              'cephadm', 'pull', max_hits=1)
            evidence = [manifest2[0]]
            if pull:
                evidence.append(pull[0])
            return done('cephadm-pull-manifest-not-found',
                        f'cephadm could not pull the container image: manifest not found{at(manifest2[0])}; '
                        'because no cluster was deployed, the teardown cluster log check failed with "No such file or directory" (tracker 73921)',
                        *evidence,
                        key='cephadm pull: manifest for quay.ceph.io/ceph-ci/ceph not found: manifest unknown')
    return unknown('none of the failures this node knows')
