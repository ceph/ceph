from setuptools import setup, find_packages
import re

module_file = open("teuthology/__init__.py").read()
metadata = dict(re.findall(r"__([a-z]+)__\s*=\s*['\"]([^'\"]*)['\"]", module_file))
long_description = open('README.rst').read()

setup(
    name='teuthology',
    version=metadata['version'],
    packages=find_packages(),
    package_data={
     'teuthology.task': ['adjust-ulimits', 'edit_sudoers.sh', 'daemon-helper'],
     'teuthology.task': ['adjust-ulimits', 'edit_sudoers.sh', 'daemon-helper'],
     'teuthology.openstack': [
         'archive-key',
         'archive-key.pub',
         'openstack-centos-6.5-user-data.txt',
         'openstack-centos-7.0-user-data.txt',
         'openstack-centos-7.1-user-data.txt',
         'openstack-centos-7.2-user-data.txt',
         'openstack-debian-8.0-user-data.txt',
         'openstack-opensuse-42.1-user-data.txt',
         'openstack-teuthology.cron',
         'openstack-teuthology.init',
         'openstack-ubuntu-12.04-user-data.txt',
         'openstack-ubuntu-14.04-user-data.txt',
         'openstack-user-data.txt',
         'openstack.yaml',
         'setup-openstack.sh'
     ],
    },
    author='Inktank Storage, Inc.',
    author_email='ceph-qa@ceph.com',
    description='Ceph test framework',
    license='MIT',
    keywords='teuthology test ceph cluster',
    url='https://github.com/ceph/teuthology',
    long_description=long_description,
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Quality Assurance',
        'Topic :: Software Development :: Testing',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Filesystems',
    ],
    install_requires=['apache-libcloud',
                      'gevent',
                      'PyYAML',
                      'argparse >= 1.2.1',
                      'configobj',
                      'six >= 1.9', # python-openstackclient won't work properly with less
                      'pexpect',
                      'docopt',
                      'netaddr',  # teuthology/misc.py
                      # only used by orchestra, but we monkey-patch it in
                      # teuthology/__init__.py
                      'paramiko',
                      'psutil >= 2.1.0',
                      'configparser',
                      'ansible>=2.0',
                      'prettytable',
                      'manhole',
                      'humanfriendly',
                      ],
    extras_require = {
        'orchestra': [
            # For apache-libcloud when using python < 2.7.9
            'backports.ssl_match_hostname',
            'beanstalkc3 >= 0.4.0',
            'httplib2',
            'ndg-httpsclient',  # for requests, urllib3
            'pyasn1',           # for requests, urllib3
            'pyopenssl>=0.13',  # for requests, urllib3
            'python-dateutil',
            # python-novaclient is specified here, even though it is
            # redundant, because python-openstackclient requires
            # Babel, and installs 2.3.3, which is forbidden by
            # python-novaclient 4.0.0
            'python-novaclient',
            'python-openstackclient',
            # with openstacklient >= 2.1.0, neutronclient no longer is
            # a dependency but we need it anyway.
            'python-neutronclient',
            'raven',
            'requests != 2.13.0',
        ],
        'test': [
            'boto >= 2.0b4',       # for qa/tasks/radosgw_*.py
            'cryptography >= 2.7',  # for qa/tasks/mgr/dashboard/test_rgw.py
            'nose', # for qa/tasks/rgw_multisite_tests.py',
            'pip-tools',
            'pytest',           # for tox.ini
            'requests',         # for qa/tasks/mgr/dashboard/helper.py
            'tox',
            # For bucket notification testing in multisite
            'xmltodict',
            'boto3',
            'PyJWT',            # for qa/tasks/mgr/dashboard/test_auth.py
            'ipy',              # for qa/tasks/cephfs/mount.py
            'toml',             # for qa/tasks/cephadm.py
        ]
    },


    # to find the code associated with entry point
    # A.B:foo first cd into directory A, open file B
    # and find sub foo
    entry_points={
        'console_scripts': [
            'teuthology = scripts.run:main',
            'teuthology-openstack = scripts.openstack:main',
            'teuthology-nuke = scripts.nuke:main',
            'teuthology-suite = scripts.suite:main',
            'teuthology-ls = scripts.ls:main',
            'teuthology-worker = scripts.worker:main',
            'teuthology-lock = scripts.lock:main',
            'teuthology-schedule = scripts.schedule:main',
            'teuthology-updatekeys = scripts.updatekeys:main',
            'teuthology-update-inventory = scripts.update_inventory:main',
            'teuthology-results = scripts.results:main',
            'teuthology-report = scripts.report:main',
            'teuthology-kill = scripts.kill:main',
            'teuthology-queue = scripts.queue:main',
            'teuthology-prune-logs = scripts.prune_logs:main',
            'teuthology-describe = scripts.describe:main',
            'teuthology-reimage = scripts.reimage:main'
            ],
        },

    )
