from setuptools import setup, find_packages


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
    tests_require=[
        'pytest >=2.1.3',
        'tox',
    ],
    scripts = ['bin/ceph-volume', 'bin/ceph-volume-systemd'],
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
