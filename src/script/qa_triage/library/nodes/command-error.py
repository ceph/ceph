import re

from qa_triage.tree.core import done, node, unknown
from qa_triage.tree.common import at

# This job, and every job the file was written for, with the path each takes.
EXAMPLES = {
    'skanta-2026-10-12_23:32:34-rados-wip-bharath20-testing-20261012.132505-main-distro-default-trial/631076': 'command/error/jerasure-invalid-technique',
    'skanta-2026-10-12_23:32:34-rados-wip-bharath20-testing-20261012.132505-main-distro-default-trial/631306': 'command/error/authtool-type2-key',
    'lflores-2026-08-20_02:51:19-upgrade-wip-umbrella-rc2-distro-default-trial/495501': 'command/error/upgraded-daemon-keyring-malformed',
    'pdonnell-2026-08-26_21:44:10-rados-wip-pdonnell-testing-20260826.182613-umbrella-distro-default-trial/517073': 'command/error/runc-mountpoint-moved',
    'skanta-2026-06-04_09:53:35-rados-wip-bharath5-testing-2026-06-02-1032-distro-default-trial/263770': 'command/error/zap-device-missing',
    'lflores-2026-03-10_15:30:45-upgrade-wip-rocky10-branch-of-the-day-2026-03-04-1772633736-66244-revert-distro-default-trial/98366': 'command/error/mgr-modules-not-loaded',
}


@node('command/error', 'What is behind the command error?')
def command_error(ctx):
    command = ctx.facts.get('command', '')
    error = ctx.facts.get('error', '')
    if command.startswith('ceph osd erasure-code-profile set'):
        found = re.search(r'technique=(\S+) is not a valid coding technique', error)
        if found:
            technique = found.group(1)
            used = ctx.search('teuthology.log',
                              r'erasure-code-profile set .*plugin=jerasure .*technique='
                              + re.escape(technique) + r'(?:\s|\'|$)',
                              'plugin=jerasure', 'erasure-code-profile set')
            said = ctx.search('teuthology.log',
                              r'stderr:Error ENOENT: technique=' + re.escape(technique)
                              + r' is not a valid coding technique',
                              'is not a valid coding technique')
            if used and said:
                return done('jerasure-invalid-technique',
                            f'the erasure-code profile was set with plugin=jerasure and '
                            f'technique={technique}{at(used[0])}, which the jerasure plugin '
                            'does not accept (it takes reed_sol_van, reed_sol_r6_op, '
                            'cauchy_orig, cauchy_good, liberation, blaum_roth, liber8tion), '
                            'so the mon refused it with ENOENT: the qa profile yaml names '
                            'a technique that only another plugin (isa) has',
                            used[0], said[0],
                            key=f'jerasure profile: technique={technique} is not valid',
                            technique=technique)
    if command.startswith('ceph-authtool') and error.startswith('error reading file'):
        typed = ctx.search('teuthology.log', r'key = Ag[A-Za-z0-9+/]{40,}', 'key = Ag')
        if typed:
            return done('authtool-type2-key',
                        f'ceph-authtool could not read the keyring{at(typed[0])}: it holds a '
                        'type-2 (aes256k) key, which the old release installed on the client '
                        'host cannot parse, because the cluster was set up with '
                        'client_key_type aes',
                        typed[0],
                        key='ceph-authtool: keyring has a type-2 key')
    # Upgrades from tentacle only: squid-x jobs fail at the same command but are
    # not yet known to have this cause.
    if 'upgrade already in progress' in error and re.search(r'--image \S+:tentacle\b', command):
        paused = ctx.search('teuthology.log', r'Upgrade: Paused due to (UPGRADE_[A-Z_]+)',
                            'Upgrade: Paused due to')
        malformed = ctx.search('teuthology.log',
                               r'auth: error parsing file (\S+/keyring): error setting modifier '
                               r'for \[([^\]]+)\] type=key val=Ag\S*: Malformed input',
                               'Malformed input')
        if paused and malformed:
            reason = re.search(r'Paused due to (UPGRADE_[A-Z_]+)', paused[0].text).group(1)
            found = re.search(r'error parsing file (\S+/keyring): error setting modifier '
                              r'for \[([^\]]+)\]', malformed[0].text)
            keyring, entity = found.group(1), found.group(2)
            kind = entity.split('.')[0]
            return done('upgraded-daemon-keyring-malformed',
                        f'{entity}, redeployed on the new image, could not parse its keyring '
                        f'{keyring}{at(malformed[0])}: it holds a type-2 (aes256k) key that '
                        'tentacle wrote and the new release reads as malformed input, '
                        f'so the daemon did not start and the upgrade paused with {reason}'
                        f'{at(paused[0])}; the next "ceph orch upgrade start" with '
                        '--daemon-types was then refused because that upgrade was still '
                        'in progress',
                        malformed[0], paused[0],
                        key=f'upgrade paused: {kind} cannot parse the type-2 key in its keyring',
                        entity=entity, paused=reason)
    if 'upgrade already in progress' in error and re.search(r'--image \S+:squid\b', command):
        unloaded = ctx.search('teuthology.log',
                              r"Module '([a-z_]+)' is not enabled/loaded \(required by command "
                              r"'orch upgrade [a-z]+'\)",
                              'is not enabled/loaded')
        paused = ctx.search('teuthology.log', r'Upgrade: Paused due to (UPGRADE_[A-Z_]+)',
                            'Upgrade: Paused due to')
        malformed = ctx.search('teuthology.log',
                               r'auth: error parsing file \S+/keyring: error setting modifier '
                               r'for \[[^\]]+\] type=key val=Ag\S*: Malformed input',
                               'Malformed input')
        if unloaded and not paused and not malformed:
            module = re.search(r"Module '([a-z_]+)'", unloaded[0].text).group(1)
            return done('mgr-modules-not-loaded',
                        f'"ceph orch upgrade status" failed{at(unloaded[0])} because the mgr '
                        f'that had just restarted had not loaded its {module} module yet '
                        '("is not enabled/loaded"), so the wait loop took the upgrade to be '
                        'over; the next "ceph orch upgrade start" with --daemon-types was then '
                        'refused because the mgr upgrade was still in progress: the mgr '
                        'advertises itself as active before its modules have loaded',
                        unloaded[0],
                        key='orch upgrade status: mgr module not loaded just after a mgr restart',
                        module=module)
    if error.startswith('docker: Error response from daemon') and 'runc create failed' in error:
        moved = ctx.search('teuthology.log',
                           r'error mounting "(\S+)" to rootfs at "(\S+)": reopen mountpoint '
                           r'after mount: mountpoint "\S+" was moved while re-opening',
                           'was moved while re-opening')
        if moved:
            found = re.search(r'error mounting "(\S+)" to rootfs at "(\S+)"', moved[0].text)
            target = found.group(2)
            return done('runc-mountpoint-moved',
                        f'docker could not start the container{at(moved[0])}: runc failed '
                        f'to bind-mount the daemon config over {target} ("reopen mountpoint '
                        'after mount: mountpoint was moved while re-opening"), so the '
                        'cephadm shell exited with docker\'s status 125 before any ceph '
                        'command ran: a mount race in the container runtime on the host',
                        moved[0],
                        key=f'runc: mountpoint {target} was moved while re-opening',
                        target=target)
    if command.startswith('cephadm ') and ' ceph-volume ' in command + ' ' \
            and error.startswith('RuntimeError: Failed command'):
        missing = ctx.search('teuthology.log',
                             r'stderr RuntimeError: (/dev/mapper/ceph--\S+) not found\.',
                             'not found.')
        listing = ctx.search('teuthology.log',
                             r'block\[1\] = UdevData\(block\[1\]\)\.preferred_block_path',
                             'UdevData(block[1])')
        if missing and listing:
            return done('zap-device-missing',
                        f'ceph-volume crashed while listing devices{at(missing[0])}: its '
                        'device scan raised "RuntimeError: /dev/mapper/ceph--... not found" for '
                        'a logical volume whose device-mapper node had vanished (it was being '
                        'deactivated), so the "lvm zap" of the OSD disk exited 1 before '
                        'zapping anything',
                        missing[0], listing[0],
                        key='ceph-volume: device scan found /dev/mapper/ceph-* missing')
    return unknown('none of the command errors this node knows')
