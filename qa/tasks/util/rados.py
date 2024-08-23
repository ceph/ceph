import logging

from teuthology import misc as teuthology
from teuthology.orchestra.run import Raw

log = logging.getLogger(__name__)

def rados(ctx, remote, cmd, wait=True, check_status=False):
    testdir = teuthology.get_testdir(ctx)
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=check_status,
        wait=wait,
        )
    if wait:
        return proc.exitstatus
    else:
        return proc

def create_ec_pool(remote, name, profile_name, pgnum, profile={}, cluster_name="ceph", application=None):
    remote.run(args=['sudo', 'ceph'] +
               cmd_erasure_code_profile(profile_name, profile) + ['--cluster', cluster_name])
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'pool', 'create', name,
        str(pgnum), str(pgnum), 'erasure', profile_name, '--cluster', cluster_name
        ])
    if application:
        remote.run(args=[
            'sudo', 'ceph', 'osd', 'pool', 'application', 'enable', name, application, '--cluster', cluster_name
        ], check_status=False) # may fail as EINVAL when run in jewel upgrade test

def create_replicated_pool(remote, name, pgnum, cluster_name="ceph", application=None):
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'pool', 'create', name, str(pgnum), str(pgnum), '--cluster', cluster_name
        ])
    if application:
        remote.run(args=[
            'sudo', 'ceph', 'osd', 'pool', 'application', 'enable', name, application, '--cluster', cluster_name
        ], check_status=False)

def create_cache_pool(remote, base_name, cache_name, pgnum, size, cluster_name="ceph"):
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'pool', 'create', cache_name, str(pgnum), '--cluster', cluster_name
    ])
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'tier', 'add-cache', base_name, cache_name,
        str(size), '--cluster', cluster_name
    ])

def cmd_erasure_code_profile(profile_name, profile):
    """
    Return the shell command to run to create the erasure code profile
    described by the profile parameter.
    
    :param profile_name: a string matching [A-Za-z0-9-_.]+
    :param profile: a map whose semantic depends on the erasure code plugin
    :returns: a shell command as an array suitable for Remote.run

    If profile is {}, it is replaced with 

      { 'k': '2', 'm': '1', 'crush-failure-domain': 'osd'}

    for backward compatibility. In previous versions of teuthology,
    these values were hardcoded as function arguments and some yaml
    files were designed with these implicit values. The teuthology
    code should not know anything about the erasure code profile
    content or semantic. The valid values and parameters are outside
    its scope.
    """

    if profile == {}:
        profile = {
            'k': '2',
            'm': '1',
            'crush-failure-domain': 'osd'
        }
    return [
        'osd', 'erasure-code-profile', 'set',
        profile_name
        ] + [ str(key) + '=' + str(value) for key, value in profile.items() ]

def cmd_ec_crush_profile(crush_name, profile):
    """
    Return the shell command to run to create the erasure code crush rule
    described by the profile parameter.
    
    :param crush_name: a string matching [A-Za-z0-9-_.]+
    :param profile: a map whose semantic depends on the crush rule syntax
    :returns: a shell command as an array suitable for Remote.run

    If profile is {}, return an empty list.
    """

    if profile == {}:
        return []

    id_value = profile.get('id', 1)
    ec_type = profile.get('type', 'erasure')
    min_size = profile.get('min_size', 1)
    max_size = profile.get('max_size', 10)
    steps = profile.get('steps', [])

    crush_content = f"rule {crush_name}-{id_value} {{"
    crush_content += f"    id {id_value}"
    crush_content += f"    type {ec_type}"
    crush_content += f"    min_size {min_size}"
    crush_content += f"    max_size {max_size}"
    for step in steps:
        crush_content += f"    step {step}"
    crush_content += "}"

    return ['osd', 'crush', 'rule', 'create-erasure', crush_name, '-i', '-', Raw("<<<"), crush_content]
