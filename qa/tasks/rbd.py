"""
Rbd testing task
"""
import contextlib
import logging
import os
import tempfile
import sys

from io import StringIO
from teuthology.orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from teuthology.task.common_fs_utils import generic_mkfs
from teuthology.task.common_fs_utils import generic_mount
from teuthology.task.common_fs_utils import default_image_name


#V1 image unsupported but required for testing purposes
os.environ["RBD_FORCE_ALLOW_V1"] = "1"

log = logging.getLogger(__name__)

ENCRYPTION_PASSPHRASE = "password"
CLONE_ENCRYPTION_PASSPHRASE = "password2"

@contextlib.contextmanager
def create_image(ctx, config):
    """
    Create an rbd image.

    For example::

        tasks:
        - ceph:
        - rbd.create_image:
            client.0:
                image_name: testimage
                image_size: 100
                image_format: 1
                encryption_format: luks2
            client.1:

    Image size is expressed as a number of megabytes; default value
    is 10240.

    Image format value must be either 1 or 2; default value is 1.

    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task create_image only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        images = config.items()
    else:
        images = [(role, None) for role in config]

    testdir = teuthology.get_testdir(ctx)
    passphrase_file = '{tdir}/passphrase'.format(tdir=testdir)
    for role, properties in images:
        if properties is None:
            properties = {}
        name = properties.get('image_name', default_image_name(role))
        size = properties.get('image_size', 10240)
        fmt = properties.get('image_format', 1)
        encryption_format = properties.get('encryption_format', 'none')
        cluster, _, _ = teuthology.split_role(role)
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Creating image {name} with size {size}'.format(name=name,
                                                                 size=size))
        args = [
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'rbd',
                '--cluster', cluster,
                '-p', 'rbd',
                'create',
                '--size', str(size),
                name,
            ]
        # omit format option if using the default (format 1)
        # since old versions of don't support it
        if int(fmt) != 1:
            args += ['--image-format', str(fmt)]
        remote.run(args=args)

        if encryption_format != 'none':
            remote.run(
                args=[
                    'echo',
                    ENCRYPTION_PASSPHRASE,
                    run.Raw('>'),
                    passphrase_file
                    ]
                )
            remote.run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'rbd',
                    '--cluster', cluster,
                    'encryption',
                    'format',
                    name,
                    encryption_format,
                    passphrase_file,
                    '-p',
                    'rbd'
                    ]
                )
    try:
        yield
    finally:
        log.info('Deleting rbd images...')
        remote.run(args=['rm', '-f', passphrase_file])
        for role, properties in images:
            if properties is None:
                properties = {}
            name = properties.get('image_name', default_image_name(role))
            cluster, _, _ = teuthology.split_role(role)
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'rbd',
                    '--cluster', cluster,
                    '-p', 'rbd',
                    'rm',
                    name,
                    ],
                )

@contextlib.contextmanager
def clone_image(ctx, config):
    """
    Clones a parent imag

    For example::

        tasks:
        - ceph:
        - rbd.clone_image:
            client.0:
                parent_name: testimage
                image_name: cloneimage
                encryption_format: luks2
    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task clone_image only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        images = config.items()
    else:
        images = [(role, None) for role in config]

    testdir = teuthology.get_testdir(ctx)
    clone_passphrase_file = '{tdir}/clone-passphrase'.format(tdir=testdir)
    for role, properties in images:
        if properties is None:
            properties = {}

        name = properties.get('image_name', default_image_name(role))
        cluster, _, _ = teuthology.split_role(role)
        parent_name = properties.get('parent_name')
        assert parent_name is not None, \
            "parent_name is required"
        parent_spec = '{name}@{snap}'.format(name=parent_name, snap=name)

        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Clone image {parent} to {child}'.format(parent=parent_name,
                                                          child=name))

        commands = [('snap', 'create', parent_spec),
                    ('snap', 'protect', parent_spec),
                    ('clone', parent_spec, name)
                    ]

        encryption_format = properties.get('encryption_format', 'none')
        if encryption_format != 'none':
            remote.run(
                args=[
                    'echo',
                    CLONE_ENCRYPTION_PASSPHRASE,
                    run.Raw('>'),
                    clone_passphrase_file
                    ]
                )

            commands.append(
                ('encryption', 'format', name, encryption_format,
                 clone_passphrase_file)
            )

        for cmd in commands:
            args = [
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'rbd', '--cluster', cluster, '-p', 'rbd'
                    ]
            args.extend(cmd)
            remote.run(args=args)

    try:
        yield
    finally:
        log.info('Deleting rbd clones...')
        remote.run(args=['rm', '-f', clone_passphrase_file])
        for role, properties in images:
            if properties is None:
                properties = {}
            name = properties.get('image_name', default_image_name(role))
            cluster, _, _ = teuthology.split_role(role)
            parent_name = properties.get('parent_name')
            parent_spec = '{name}@{snap}'.format(name=parent_name, snap=name)

            (remote,) = ctx.cluster.only(role).remotes.keys()

            for cmd in [('rm', name),
                        ('snap', 'unprotect', parent_spec),
                        ('snap', 'rm', parent_spec)]:
                args = [
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'rbd', '--cluster', cluster, '-p', 'rbd'
                        ]
                args.extend(cmd)
                remote.run(args=args)

@contextlib.contextmanager
def modprobe(ctx, config):
    """
    Load the rbd kernel module..

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
    """
    log.info('Loading rbd kernel module...')
    for role in config:
        (remote,) = ctx.cluster.only(role).remotes.keys()
        remote.run(
            args=[
                'sudo',
                'modprobe',
                'rbd',
                ],
            )
    try:
        yield
    finally:
        log.info('Unloading rbd kernel module...')
        for role in config:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'modprobe',
                    '-r',
                    'rbd',
                    # force errors to be ignored; necessary if more
                    # than one device was created, which may mean
                    # the module isn't quite ready to go the first
                    # time through.
                    run.Raw('||'),
                    'true',
                    ],
                )

@contextlib.contextmanager
def dev_create(ctx, config):
    """
    Map block devices to rbd images.

    For example::

        tasks:
        - ceph:
        - rbd.create_image: [client.0]
        - rbd.modprobe: [client.0]
        - rbd.dev_create:
            client.0:
                image_name: testimage.client.0
                encryption_format: luks2
                parent_encryption_format: luks1
    """
    assert isinstance(config, dict) or isinstance(config, list), \
        "task dev_create only supports a list or dictionary for configuration"

    if isinstance(config, dict):
        images = config.items()
    else:
        images = [(role, None) for role in config]

    log.info('Creating rbd block devices...')

    testdir = teuthology.get_testdir(ctx)
    passphrase_file = '{tdir}/passphrase'.format(tdir=testdir)
    clone_passphrase_file = '{tdir}/clone-passphrase'.format(tdir=testdir)
    device_path = {}

    for role, properties in images:
        if properties is None:
            properties = {}
        name = properties.get('image_name', default_image_name(role))
        cluster, _, _ = teuthology.split_role(role)
        parent_encryption_format = properties.get('parent_encryption_format',
                                                  'none')
        encryption_format = properties.get('encryption_format',
                                           parent_encryption_format)
        (remote,) = ctx.cluster.only(role).remotes.keys()

        if encryption_format == 'none' and parent_encryption_format == 'none':
            device_path[role] = '/dev/rbd/rbd/{image}'.format(image=name)
            device_specific_args = []
        else:
            device_specific_args = ['-t', 'nbd', '-o']

            is_cloned = properties.get('parent_name') is not None
            encryption_args = ""
            if is_cloned and properties.get('encryption_format') != 'none':
                remote.run(
                    args=[
                        'echo',
                        CLONE_ENCRYPTION_PASSPHRASE,
                        run.Raw('>'),
                        clone_passphrase_file
                        ]
                    )

                encryption_args = \
                    'encryption-format=%s,encryption-passphrase-file=%s' % (
                        encryption_format, clone_passphrase_file)

            if not is_cloned or parent_encryption_format != 'none':
                remote.run(
                    args=[
                        'echo',
                        ENCRYPTION_PASSPHRASE,
                        run.Raw('>'),
                        passphrase_file
                        ]
                    )

                if is_cloned and properties.get('encryption_format') != 'none':
                    encryption_args += ","

                if parent_encryption_format != 'none':
                    encryption_args += \
                        'encryption-format=%s,encryption-passphrase-file=%s' % (
                            parent_encryption_format, passphrase_file)
                else:
                    encryption_args += \
                        'encryption-format=%s,encryption-passphrase-file=%s' % (
                            encryption_format, passphrase_file)

            device_specific_args.append(encryption_args)

        map_fp = StringIO()
        remote.run(
            args=[
                'sudo',
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'rbd',
                '--cluster', cluster,
                '--id', role.rsplit('.')[-1],
                '-p', 'rbd',
                'map',
                name] + device_specific_args,
            stdout=map_fp,
            )

        if encryption_format != 'none' or parent_encryption_format != 'none':
            device_path[role] = map_fp.getvalue().rstrip()
            properties['device_path'] = device_path[role]
            remote.run(args=['sudo', 'chmod', '666', device_path[role]])
    try:
        yield
    finally:
        log.info('Unmapping rbd devices...')
        remote.run(args=['rm', '-f', passphrase_file, clone_passphrase_file])
        for role, properties in images:
            if not device_path.get(role):
                continue

            if properties is None:
                properties = {}
            encryption_format = properties.get('encryption_format', 'none')
            parent_encryption_format = properties.get(
                'parent_encryption_format', 'none')
            (remote,) = ctx.cluster.only(role).remotes.keys()

            if encryption_format == 'none' and \
                    parent_encryption_format == 'none':
                device_specific_args = []
            else:
                device_specific_args = ['-t', 'nbd']

            remote.run(
                args=[
                    'LD_LIBRARY_PATH={tdir}/binary/usr/local/lib'.format(tdir=testdir),
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'rbd',
                    '-p', 'rbd',
                    'unmap',
                    device_path[role],
                    ] + device_specific_args,
                )


def rbd_devname_rtn(ctx, image):
    return '/dev/rbd/rbd/{image}'.format(image=image)    

def canonical_path(ctx, role, path):
    """
    Determine the canonical path for a given path on the host
    representing the given role.  A canonical path contains no
    . or .. components, and includes no symbolic links.
    """
    version_fp = StringIO()
    ctx.cluster.only(role).run(
        args=[ 'readlink', '-f', path ],
        stdout=version_fp,
        )
    canonical_path = version_fp.getvalue().rstrip('\n')
    version_fp.close()
    return canonical_path

@contextlib.contextmanager
def run_xfstests(ctx, config):
    """
    Run xfstests over specified devices.

    Warning: both the test and scratch devices specified will be
    overwritten.  Normally xfstests modifies (but does not destroy)
    the test device, but for now the run script used here re-makes
    both filesystems.

    Note: Only one instance of xfstests can run on a single host at
    a time, although this is not enforced.

    This task in its current form needs some improvement.  For
    example, it assumes all roles provided in the config are
    clients, and that the config provided is a list of key/value
    pairs.  For now please use the xfstests() interface, below.

    For example::

        tasks:
        - ceph:
        - rbd.run_xfstests:
            client.0:
                count: 2
                test_dev: 'test_dev'
                scratch_dev: 'scratch_dev'
                fs_type: 'xfs'
                tests: 'generic/100 xfs/003 xfs/005 xfs/006 generic/015'
                exclude:
                - generic/42
                randomize: true
    """
    with parallel() as p:
        for role, properties in config.items():
            p.spawn(run_xfstests_one_client, ctx, role, properties)
        exc = None
        while True:
            try:
                p.next()
            except StopIteration:
                break
            except:
                exc = sys.exc_info()[1]
        if exc is not None:
            raise exc
    yield

def run_xfstests_one_client(ctx, role, properties):
    """
    Spawned routine to handle xfs tests for a single client
    """
    testdir = teuthology.get_testdir(ctx)
    try:
        count = properties.get('count')
        test_dev = properties.get('test_dev')
        assert test_dev is not None, \
            "task run_xfstests requires test_dev to be defined"
        test_dev = canonical_path(ctx, role, test_dev)

        scratch_dev = properties.get('scratch_dev')
        assert scratch_dev is not None, \
            "task run_xfstests requires scratch_dev to be defined"
        scratch_dev = canonical_path(ctx, role, scratch_dev)

        fs_type = properties.get('fs_type')
        tests = properties.get('tests')
        exclude_list = properties.get('exclude')
        randomize = properties.get('randomize')

        (remote,) = ctx.cluster.only(role).remotes.keys()

        # Fetch the test script
        test_root = teuthology.get_testdir(ctx)
        test_script = 'run_xfstests.sh'
        test_path = os.path.join(test_root, test_script)

        xfstests_url = properties.get('xfstests_url')
        assert xfstests_url is not None, \
            "task run_xfstests requires xfstests_url to be defined"

        xfstests_krbd_url = xfstests_url + '/' + test_script

        log.info('Fetching {script} for {role} from {url}'.format(
            script=test_script,
            role=role,
            url=xfstests_krbd_url))

        args = [ 'wget', '-O', test_path, '--', xfstests_krbd_url ]
        remote.run(args=args)

        log.info('Running xfstests on {role}:'.format(role=role))
        log.info('   iteration count: {count}:'.format(count=count))
        log.info('       test device: {dev}'.format(dev=test_dev))
        log.info('    scratch device: {dev}'.format(dev=scratch_dev))
        log.info('     using fs_type: {fs_type}'.format(fs_type=fs_type))
        log.info('      tests to run: {tests}'.format(tests=tests))
        log.info('      exclude list: {}'.format(' '.join(exclude_list)))
        log.info('         randomize: {randomize}'.format(randomize=randomize))

        if exclude_list:
            with tempfile.NamedTemporaryFile(mode='w', prefix='exclude') as exclude_file:
                for test in exclude_list:
                    exclude_file.write("{}\n".format(test))
                exclude_file.flush()
                remote.put_file(exclude_file.name, exclude_file.name)

        # Note that the device paths are interpreted using
        # readlink -f <path> in order to get their canonical
        # pathname (so it matches what the kernel remembers).
        args = [
            '/usr/bin/sudo',
            'TESTDIR={tdir}'.format(tdir=testdir),
            'adjust-ulimits',
            'ceph-coverage',
            '{tdir}/archive/coverage'.format(tdir=testdir),
            '/bin/bash',
            test_path,
            '-c', str(count),
            '-f', fs_type,
            '-t', test_dev,
            '-s', scratch_dev,
            ]
        if exclude_list:
            args.extend(['-x', exclude_file.name])
        if randomize:
            args.append('-r')
        if tests:
            args.extend(['--', tests])
        remote.run(args=args, logger=log.getChild(role))
    finally:
        log.info('Removing {script} on {role}'.format(script=test_script,
                                                      role=role))
        remote.run(args=['rm', '-f', test_path])

@contextlib.contextmanager
def xfstests(ctx, config):
    """
    Run xfstests over rbd devices.  This interface sets up all
    required configuration automatically if not otherwise specified.
    Note that only one instance of xfstests can run on a single host
    at a time.  By default, the set of tests specified is run once.
    If a (non-zero) count value is supplied, the complete set of
    tests will be run that number of times.

    For example::

        tasks:
        - ceph:
        # Image sizes are in MB
        - rbd.xfstests:
            client.0:
                count: 3
                test_image: 'test_image'
                test_size: 250
                test_format: 2
                scratch_image: 'scratch_image'
                scratch_size: 250
                scratch_format: 1
                fs_type: 'xfs'
                tests: 'generic/100 xfs/003 xfs/005 xfs/006 generic/015'
                exclude:
                - generic/42
                randomize: true
                xfstests_url: 'https://raw.github.com/ceph/ceph-ci/wip-55555/qa'
    """
    if config is None:
        config = { 'all': None }
    assert isinstance(config, dict) or isinstance(config, list), \
        "task xfstests only supports a list or dictionary for configuration"
    if isinstance(config, dict):
        config = teuthology.replace_all_with_clients(ctx.cluster, config)
        runs = config.items()
    else:
        runs = [(role, None) for role in config]

    running_xfstests = {}
    for role, properties in runs:
        cluster, typ, _ = teuthology.split_role(role)
        assert typ == "client", \
            "task xfstests can only run on client nodes"
        for host, roles_for_host in ctx.cluster.remotes.items():
            if role in roles_for_host:
                assert host not in running_xfstests, \
                    "task xfstests allows only one instance at a time per host"
                running_xfstests[host] = True

    images_config = {}
    scratch_config = {}
    modprobe_config = {}
    image_map_config = {}
    scratch_map_config = {}
    xfstests_config = {}
    for role, properties in runs:
        if properties is None:
            properties = {}

        test_image = properties.get('test_image', 'test_image.{role}'.format(role=role))
        test_size = properties.get('test_size', 10000) # 10G
        test_fmt = properties.get('test_format', 1)
        scratch_image = properties.get('scratch_image', 'scratch_image.{role}'.format(role=role))
        scratch_size = properties.get('scratch_size', 10000) # 10G
        scratch_fmt = properties.get('scratch_format', 1)

        images_config[role] = dict(
            image_name=test_image,
            image_size=test_size,
            image_format=test_fmt,
            )

        scratch_config[role] = dict(
            image_name=scratch_image,
            image_size=scratch_size,
            image_format=scratch_fmt,
            )

        xfstests_branch = properties.get('xfstests_branch', 'master')
        xfstests_url = properties.get('xfstests_url', 'https://raw.github.com/ceph/ceph/{branch}/qa'.format(branch=xfstests_branch))

        xfstests_config[role] = dict(
            count=properties.get('count', 1),
            test_dev='/dev/rbd/rbd/{image}'.format(image=test_image),
            scratch_dev='/dev/rbd/rbd/{image}'.format(image=scratch_image),
            fs_type=properties.get('fs_type', 'xfs'),
            randomize=properties.get('randomize', False),
            tests=properties.get('tests'),
            exclude=properties.get('exclude', []),
            xfstests_url=xfstests_url,
            )

        log.info('Setting up xfstests using RBD images:')
        log.info('      test ({size} MB): {image}'.format(size=test_size,
                                                        image=test_image))
        log.info('   scratch ({size} MB): {image}'.format(size=scratch_size,
                                                        image=scratch_image))
        modprobe_config[role] = None
        image_map_config[role] = {'image_name': test_image}
        scratch_map_config[role] = {'image_name': scratch_image}

    with contextutil.nested(
        lambda: create_image(ctx=ctx, config=images_config),
        lambda: create_image(ctx=ctx, config=scratch_config),
        lambda: modprobe(ctx=ctx, config=modprobe_config),
        lambda: dev_create(ctx=ctx, config=image_map_config),
        lambda: dev_create(ctx=ctx, config=scratch_map_config),
        lambda: run_xfstests(ctx=ctx, config=xfstests_config),
        ):
        yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Create and mount an rbd image.

    For example, you can specify which clients to run on::

        tasks:
        - ceph:
        - rbd: [client.0, client.1]

    There are a few image options::

        tasks:
        - ceph:
        - rbd:
            client.0: # uses defaults
            client.1:
                image_name: foo
                image_size: 2048
                image_format: 2
                fs_type: xfs

    To use default options on all clients::

        tasks:
        - ceph:
        - rbd:
            all:

    To create 20GiB images and format them with xfs on all clients::

        tasks:
        - ceph:
        - rbd:
            all:
              image_size: 20480
              fs_type: xfs
    """
    if config is None:
        config = { 'all': None }
    norm_config = config
    if isinstance(config, dict):
        norm_config = teuthology.replace_all_with_clients(ctx.cluster, config)
    if isinstance(norm_config, dict):
        role_images = {}
        for role, properties in norm_config.items():
            if properties is None:
                properties = {}
            role_images[role] = properties.get('image_name')
    else:
        role_images = norm_config

    log.debug('rbd config is: %s', norm_config)

    with contextutil.nested(
        lambda: create_image(ctx=ctx, config=norm_config),
        lambda: modprobe(ctx=ctx, config=norm_config),
        lambda: dev_create(ctx=ctx, config=norm_config),
        lambda: generic_mkfs(ctx=ctx, config=norm_config,
                devname_rtn=rbd_devname_rtn),
        lambda: generic_mount(ctx=ctx, config=role_images,
                devname_rtn=rbd_devname_rtn),
        ):
        yield
