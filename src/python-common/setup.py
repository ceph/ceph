import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


if sys.version_info >= (3,0):
    mypy = ['mypy', 'pytest-mypy']
    pytest = ['pytest >=2.1.3']
else:
    mypy = []
    pytest = ['pytest >=2.1.3,<5', 'mock']


with open("README.rst", "r") as fh:
    long_description = fh.read()


class PyTest(TestCommand):
    user_options = [('addopts=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.addopts = []

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest

        args = self.addopts.split() if isinstance(self.addopts, str) else self.addopts
        errno = pytest.main(args)
        sys.exit(errno)


setup(
    name='ceph',
    version='1.0.0',
    packages=find_packages(),
    author='',
    author_email='dev@ceph.io',
    description='Ceph common library',
    long_description=long_description,
    license='LGPLv2+',
    keywords='ceph',
    url="https://github.com/ceph/ceph",
    zip_safe = False,
    cmdclass={'pytest': PyTest},
    install_requires=(
        'six',
    ),
    tests_require=[
        'tox',
        'pyyaml'
    ] + mypy + pytest,
    classifiers = [
        'Intended Audience :: Developer',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
