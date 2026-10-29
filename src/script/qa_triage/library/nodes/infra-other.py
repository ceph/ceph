import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'nmordech-2026-01-26_18:32:32-rados:cephadm-wip-rocky10-branch-of-the-day-2026-01-23-1769128778-distro-default-trial/18789': 'infra/other/nvme-cli-host-identity',
    'skanta-2026-09-20_02:51:43-rados-wip-bharath22-testing-20260920.002814-main-distro-default-trial/570117': 'infra/other/nvme-cli-host-identity',
    'skanta-2026-09-25_03:59:04-rados-wip-bharath29-testing-20260924.231744-main-distro-default-trial/581509': 'infra/other/chacra-http-error',
    'skanta-2026-09-25_03:59:04-rados-wip-bharath29-testing-20260924.231744-main-distro-default-trial/581719': 'infra/other/kernel-reconnect-timeout',
    'skanta-2026-08-13_04:40:13-rados-wip-bharath8-testing-20260813.003211-main-distro-default-trial/464220': 'infra/other/nvme-list-json-failed',
    'yuriw-2026-07-28_17:11:12-rados-wip-yuri3-testing-20260728.135610-main-distro-default-trial/406415': 'infra/other/nvme-list-json-failed',
    'pdonnell-2026-07-30_17:55:58-rgw-wip-pdonnell-testing-20260730.142537-distro-default-trial/414223': 'infra/other/dpkg-file-conflict',
    'skanta-2026-06-04_15:14:33-rados-wip-bharath7-testing-2026-06-04-1630-distro-default-trial/265561': 'infra/other/chacra-dns-failure',
    'skanta-2026-09-24_03:23:39-rados-wip-bharath10-testing-20260924.010810-main-distro-default-trial/577244': 'infra/other/ansible-disk-check-trial114',
    'skanta-2026-09-24_03:23:39-rados-wip-bharath10-testing-20260924.010810-main-distro-default-trial/577254': 'infra/other/kernel-version-mismatch',
}

# dpkg refusing to unpack a deb that ships a file another package already owns.
OVERWRITE = r"trying to overwrite '(\S+)', which is also in package (\S+)"
UNPACK = r'dpkg: error processing archive \S*?/(?:\d+-)?([^/_\s]+)_[^/\s]+\.deb'

# The node could not look up the chacra host name.
CHACRA_DNS = (r"Failed to resolve '([^']*chacra\.ceph\.com)'.*"
              r'Temporary failure in name resolution')


@node('infra/other', 'What is behind the failure of the setup task?')
def infra_other(ctx):
    if ctx.facts.get('task') == 'nvme_loop':
        # Check the list failure first: if 'nvme list -o json command failed,
        # retrying...' is present, the list loop is what raised MaxWhileTries
        # (tracker 77106). The hostnqn connect error (tracker 71816) may also
        # appear on the same job if the first connect attempt with '-q hostnqn'
        # failed before the fallback without '-q' succeeded, but the connects
        # did succeed in the end; the list failure is the actual cause.
        list_failed = ctx.search('teuthology.log',
                                 r'nvme list -o json command failed, retrying\.\.\.', 
                                 'nvme list -o json command failed, retrying')
        if list_failed:
            return done('nvme-list-json-failed',
                        f'nvme_loop: nvme list -o json crashed on every attempt{at(list_failed[0])}'
                        ': the loop devices were connected but nvme-cli list fails',
                        list_failed[0],
                        key='nvme_loop: nvme list -o json command failed')
        placeholder = ctx.search('teuthology.log', r'nvme connect -t loop -n \S+ -q hostnqn',
                                 'nvme connect -t loop')
        invalid = ctx.search('teuthology.log',
                             r'stderr:Failed to write to /dev/nvme-fabrics: Invalid argument',
                             'Failed to write to /dev/nvme-fabrics')
        if placeholder and (len(invalid) >= 2 or (invalid and 'status 234' in ctx.job.reason)):
            return done('nvme-cli-host-identity',
                        f'nvme connect to the loop target was rejected by the kernel with Invalid '
                        f'argument{at(invalid[0])}: nvme-cli passes an inconsistent HostNQN/HostID pair',
                        placeholder[0], *invalid,
                        key='nvme_loop: nvme connect: Failed to write to /dev/nvme-fabrics')
    if 'chacra.ceph.com' in ctx.job.reason:
        hit = ctx.search('teuthology.log',
                         r'HTTPError.*chacra\.ceph\.com',
                         'chacra.ceph.com')
        if hit:
            return done('chacra-http-error',
                        f'the install task could not fetch the repo from chacra.ceph.com'
                        f'{at(hit[0])}: chacra returned an HTTP error',
                        hit[0],
                        key='install: chacra.ceph.com HTTP error',
                        error='chacra-http-error')
        dns = ctx.search('teuthology.log', CHACRA_DNS,
                         'Failed to resolve', 'Temporary failure in name resolution')
        if dns:
            return done('chacra-dns-failure',
                        f'the install task could not fetch the repo from chacra.ceph.com'
                        f'{at(dns[0])}: the node failed to resolve the chacra host name '
                        '(Temporary failure in name resolution)',
                        dns[0],
                        key='install: chacra.ceph.com name resolution failure',
                        error='chacra-dns-failure')
    if ctx.facts.get('task') == 'install':
        # apt-get exits 100 when dpkg cannot unpack a deb because it ships a
        # file that another installed package owns: two packages of the build
        # list the same file, a packaging bug.
        overwrite = ctx.search('teuthology.log', OVERWRITE, 'trying to overwrite')
        unpack = ctx.search('teuthology.log', UNPACK, 'dpkg: error processing archive')
        if overwrite and unpack:
            conflict = re.search(OVERWRITE, overwrite[0].text)
            archive = re.search(UNPACK, unpack[0].text)
            if conflict and archive:
                path, owner = conflict.group(1), conflict.group(2)
                package = archive.group(1)
                return done('dpkg-file-conflict',
                            f'apt-get could not install {package}{at(unpack[0])}: dpkg refused to '
                            f'unpack it because it ships {path}, which package {owner} also '
                            'ships: the two packages of the build claim the same file',
                            unpack[0], overwrite[0],
                            key=f'install: dpkg: {package} overwrites a file of {owner}',
                            package=package, owner=owner, overwritten=path)
    if ctx.facts.get('task') == 'ansible.cephlab':
        # Ansible disk-check failure on trial114: FOG kernelDevice not set,
        # OS installed on OSD drive, leaving fewer NVMe disks than expected.
        ansible_fail = ctx.search('teuthology.log',
                                  r'AnsibleFailedError.*Wanted \d+ disks of .*but only matched \d+',
                                  'AnsibleFailedError', 'Wanted')
        trial114_hit = ctx.search('teuthology.log', r'trial114', 'trial114')
        if ansible_fail and trial114_hit:
            return done('ansible-disk-check-trial114',
                        f'the ansible.cephlab disk check failed on trial114{at(ansible_fail[0])}: '
                        'trial114 had fewer non-rotational disks than expected because '
                        'the OS was installed on an OSD drive (FOG kernelDevice not set)',
                        ansible_fail[0], trial114_hit[0],
                        key='ansible.cephlab: AnsibleFailedError: Wanted N disks: trial114',
                        host='trial114')
    if ctx.facts.get('task') == 'kernel':
        reconnect = ctx.search('teuthology.log',
                               r"'reconnect to (\S+)' reached maximum tries",
                               "'reconnect to", 'reached maximum tries')
        if reconnect:
            host_match = re.search(r"'reconnect to (\S+)' reached maximum tries",
                                   reconnect[0].text)
            host = host_match.group(1) if host_match else 'a host'
            return done('kernel-reconnect-timeout',
                        f'the kernel task rebooted {host} but it did not come back '
                        f'within the reconnect timeout{at(reconnect[0])}: '
                        f'the machine failed to return after reboot',
                        reconnect[0],
                        key='kernel: wait_for_reboot: Failed to reconnect')
        # Host came back but is running the wrong kernel version: it booted from
        # the wrong device because kernelDevice is not set correctly in FOG.
        version_assert = ctx.search('teuthology.log',
                                    r'AssertionError: failed to install new kernel version within timeout',
                                    'failed to install new kernel version within timeout')
        if version_assert:
            version_mismatch = ctx.search('teuthology.log',
                                          r'current kernel version is (\S+) vs (\S+)',
                                          'current kernel version is', max_hits=10)
            for hit in version_mismatch:
                m = re.search(r'current kernel version is (\S+) vs (\S+)', hit.text)
                if m and m.group(1) != m.group(2):
                    stdout_hit = ctx.search('teuthology.log',
                                            r'orchestra\.run\.(\S+?)\.stdout:'
                                            + re.escape(m.group(1)),
                                            m.group(1))
                    host = ''
                    if stdout_hit:
                        hm = re.search(r'orchestra\.run\.(\S+?)\.stdout:', stdout_hit[0].text)
                        host = hm.group(1) if hm else ''
                    return done('kernel-version-mismatch',
                                f'the kernel task rebooted {host or "a host"} but it came back '
                                f'running {m.group(1)} instead of {m.group(2)}{at(hit)}: '
                                'the machine booted the wrong kernel, likely from the wrong '
                                'device because kernelDevice is not set correctly in FOG',
                                hit,
                                key='kernel: wait_for_reboot: wrong kernel version after reboot',
                                host=host)
    return unknown('none of the failures this node knows')
