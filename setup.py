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
     'teuthology.task': ['valgrind.supp', 'adjust-ulimits', 'edit_sudoers.sh', 'daemon-helper'],
     'teuthology': ['ceph.conf.template'],
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
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Quality Assurance',
        'Topic :: Software Development :: Testing',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Filesystems',
    ],
    install_requires=['setuptools',
                      'gevent == 0.13.6',  # 0.14 switches to libev, that means bootstrap needs to change too
                      'MySQL-python == 1.2.3',
                      'PyYAML',
                      'argparse >= 1.2.1',
                      'beanstalkc >= 0.2.0',
                      'boto >= 2.0b4',
                      'bunch >= 1.0.0',
                      'configobj',
                      'six >= 1.9', # python-openstackclient won't work properly with less
                      'httplib2',
                      'paramiko < 1.8',
                      'pexpect',
                      'requests >= 2.3.0',
                      'raven',
                      'web.py',
                      'docopt',
                      'psutil >= 2.1.0',
                      'configparser',
                      'pytest',
                      'ansible==1.9.2',
                      'pyopenssl>=0.13',
                      'ndg-httpsclient',
                      'pyasn1',
                      'python-openstackclient',
                      ],


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
            'teuthology-coverage = scripts.coverage:main',
            'teuthology-results = scripts.results:main',
            'teuthology-report = scripts.report:main',
            'teuthology-kill = scripts.kill:main',
            'teuthology-queue = scripts.queue:main',
            ],
        },

    )
