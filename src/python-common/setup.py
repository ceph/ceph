from setuptools import setup, find_packages


setup(
    name='ceph',
    version='1.0.0',
    packages=find_packages(),
    author='',
    author_email='dev@ceph.io',
    description='Ceph common library',
    license='LGPLv2+',
    keywords='ceph',
    url="https://github.com/ceph/ceph",
    zip_safe = False,
    install_requires=(
        'six',
    ),
    tests_require=[
        'pytest >=2.1.3',
        'tox',
    ],
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
