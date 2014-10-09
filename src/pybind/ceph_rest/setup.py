from setuptools import setup, find_packages
import os


def long_description():
    readme = os.path.join(os.path.dirname(__file__), 'README.rst')
    return open(readme).read()


setup(
    name = 'python-ceph-rest',
    description = 'bindings to the Ceph rest API',
    packages=find_packages(),
    author = 'Inktank',
    author_email = 'ceph-devel@vger.kernel.org',
    version = '0.80.6',
    license = "LGPL2",
    zip_safe = False,
    keywords = "ceph, rest, bindings, api, cli",
    install_requires = ['flask==0.10', 'python-rados'],
    long_description = long_description(),
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Topic :: System :: Filesystems',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
)
