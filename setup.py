from setuptools import setup, find_packages

setup(
    name='teuthology',
    version='0.0.1',
    packages=find_packages(),

    author='Tommi Virtanen',
    author_email='tommi.virtanen@dreamhost.com',
    description='Ceph test runner',
    license='MIT',
    keywords='ceph testing ssh cluster',

    # to find the code associated with entry point
    # A.B:foo first cd into directory A, open file B
    # and find sub foo
    entry_points={
        'console_scripts': [
            'teuthology = teuthology.run:main',
            'teuthology-nuke = scripts.nuke:main',
            'teuthology-suite = scripts.suite:main',
            'teuthology-ls = scripts.ls:main',
            'teuthology-worker = scripts.worker:main',
            'teuthology-lock = scripts.lock:main',
            'teuthology-schedule = teuthology.run:schedule',
            'teuthology-updatekeys = teuthology.lock:update_hostkeys',
            'teuthology-coverage = teuthology.coverage:analyze',
            'teuthology-results = teuthology.suite:results',
            'teuthology-report = teuthology.report:main',
            ],
        },

    )
