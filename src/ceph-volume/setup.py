from setuptools import setup, find_packages
import os


setup(
    name='ceph-volume',
    version='1.0.0',
    packages=find_packages(),

    author='',
    author_email='contact@redhat.com',
    description='Deploy Ceph OSDs using different device technologies like lvm or physical disks',
    license='LGPLv2+',
    keywords='ceph volume disk devices lvm',
    url="https://github.com/ceph/ceph",
    zip_safe = False,
    install_requires='ceph',
    dependency_links=[''.join(['file://', os.path.join(os.getcwd(), '../',
                                                       'python-common#egg=ceph-1.0.0')])],
    tests_require=[
        'pytest >=2.1.3',
        'tox',
    ],
    entry_points = dict(
        console_scripts = [
            'ceph-volume = ceph_volume.main:Volume',
            'ceph-volume-systemd = ceph_volume.systemd:main',
        ],
    ),
    classifiers = [
        'Environment :: Console',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Operating System :: POSIX :: Linux',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
