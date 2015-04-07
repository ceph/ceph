import os
from setuptools import setup, find_packages

# link ceph-disk script here so we can "install" it
current_dir = os.path.abspath(os.path.dirname(__file__))
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
script_path = os.path.join(src_dir, 'ceph-disk')


def link_target(source, destination):
    if not os.path.exists(destination):
        try:
            os.symlink(source, destination)
        except (IOError, OSError) as error:
            print 'Ignoring linking of target: %s' % str(error)

link_target(script_path, 'ceph_disk.py')

setup(
    name='ceph_disk',
    version='0.1',
    description='',
    author='',
    author_email='',
    zip_safe=False,
    packages=find_packages(),
)
