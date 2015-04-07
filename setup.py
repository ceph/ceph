from setuptools import setup, find_packages
import re

module_file = open("teuthology/__init__.py").read()
metadata = dict(re.findall(r"__([a-z]+)__\s*=\s*['\"]([^'\"]*)['\"]", module_file))
long_description = open('README.rst').read()

install_requires=[
    'setuptools',
    ]

install_requires.extend(
    [ln.strip() for ln in open('requirements.txt').readlines() if ln and '#'
     not in ln]
)


setup(
    name='teuthology',
    version=metadata['version'],
    packages=find_packages(),

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
    install_requires=install_requires,
    tests_require=['nose >=1.0.0', 'fudge >=1.0.3'],


    # to find the code associated with entry point
    # A.B:foo first cd into directory A, open file B
    # and find sub foo
    entry_points={
        'console_scripts': [
            'teuthology = scripts.run:main',
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
