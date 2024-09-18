from setuptools import setup, find_packages
import os


setup(
    name='ceph-node-proxy',
    version='1.0.0',
    packages=find_packages(),

    author='',
    author_email='gabrioux@ibm.com',
    description='node-proxy agent to inventory and report hardware statuses.',
    license='LGPLv2+',
    keywords='ceph hardware inventory monitoring',
    url='https://github.com/ceph/ceph',
    zip_safe=False,
    install_requires='ceph',
    dependency_links=[''.join(['file://', os.path.join(os.getcwd(), '../',
                                                       'python-common#egg=ceph-1.0.0')])],
    tests_require=[
        'pytest >=2.1.3',
        'tox',
        'ceph',
    ],
    entry_points=dict(
        console_scripts=[
            'ceph-node-proxy = ceph_node_proxy.main:main',
        ],
    ),
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.9',
    ]
)
