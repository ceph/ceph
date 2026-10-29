from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job and the others in the same run that have this failure.
EXAMPLES = {
    'skanta-2026-06-04_09:53:35-rados-wip-bharath5-testing-2026-06-02-1032-distro-default-trial/263741':
        'infra/packages/epel-mirror-404',
    'skanta-2026-06-04_09:53:35-rados-wip-bharath5-testing-2026-06-02-1032-distro-default-trial/263893':
        'infra/packages/epel-mirror-404',
    'skanta-2026-06-04_09:53:35-rados-wip-bharath5-testing-2026-06-02-1032-distro-default-trial/263901':
        'infra/packages/epel-mirror-404',
}


@node('infra/packages', 'Why did installing packages fail?')
def infra_packages(ctx):
    epel = ctx.search('teuthology.log',
                      r"Failed to download metadata for repo 'epel'.*Cannot download",
                      "Failed to download metadata for repo 'epel'")
    if epel:
        return done('epel-mirror-404',
                    'the EPEL repo metadata could not be downloaded: all mirrors '
                    'returned 404 for the metadata file' + at(epel[0]),
                    epel[0],
                    key='epel repo metadata: all mirrors 404')
    return unknown('the package install failure is not one this node knows')
