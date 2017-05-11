import logging

from teuthology import misc as teuthology

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

def create_ec_pool(remote, name, profile_name, pgnum, profile={}):
    remote.run(args=['sudo', 'ceph'] +
               cmd_erasure_code_profile(profile_name, profile))
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'pool', 'create', name,
        str(pgnum), str(pgnum), 'erasure', profile_name,
        ])

def create_replicated_pool(remote, name, pgnum):
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'pool', 'create', name, str(pgnum), str(pgnum),
        ])

def create_cache_pool(remote, base_name, cache_name, pgnum, size):
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'pool', 'create', cache_name, str(pgnum)
    ])
    remote.run(args=[
        'sudo', 'ceph', 'osd', 'tier', 'add-cache', base_name, cache_name,
        str(size),
    ])

def cmd_erasure_code_profile(profile_name, profile):
    """
    Return the shell command to run to create the erasure code profile
    described by the profile parameter.
    
    :param profile_name: a string matching [A-Za-z0-9-_.]+
    :param profile: a map whose semantic depends on the erasure code plugin
    :returns: a shell command as an array suitable for Remote.run

    If profile is {}, it is replaced with 

      { 'k': '2', 'm': '1', 'ruleset-failure-domain': 'osd'}

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
            'ruleset-failure-domain': 'osd'
        }
    return [
        'osd', 'erasure-code-profile', 'set',
        profile_name
        ] + [ str(key) + '=' + str(value) for key, value in profile.iteritems() ]
