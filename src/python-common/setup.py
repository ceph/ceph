import sys

from setuptools import setup, find_packages


if sys.version_info >= (3,0):
    mypy = ['mypy', 'pytest-mypy']
    pytest = 'pytest >=2.1.3'
else:
    mypy = []
    pytest = 'pytest >=2.1.3,<5'


with open("README.rst", "r") as fh:
    long_description = fh.read()


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
    install_requires=(
        'six',
    ),
    setup_requires=['pytest-runner'],
    tests_require=[
        pytest,
        'tox',
    ] + mypy,
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
