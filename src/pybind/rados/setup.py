# Largely taken from
# https://blog.kevin-brown.com/programming/2014/09/24/combining-autotools-and-setuptools.html
import os
import subprocess
import sys
from distutils.core import setup
from distutils.extension import Extension

from Cython.Build import cythonize

# PEP 440 versioning of the Rados package on PyPI
# Bump this version, after every changeset
# NOTE: This version is not the same as get_ceph_version()

__version__ = '0.0.1'


def get_ceph_version():
    try:
        for line in open(os.path.join(os.path.dirname(__file__), "..", "..", "ceph_ver.h")):
            if "CEPH_GIT_NICE_VER" in line:
                return line.split()[2].strip('"')
        else:
            return "0"
    except IOError:
        return "0"


def get_python_flags():
    cflags = {'I': [], 'extras': []}
    ldflags = {'l': [], 'L': [], 'extras': []}

    if os.environ.get('VIRTUAL_ENV', None):
        python = "python"
    else:
        python = 'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)

    python_config = python + '-config'

    for cflag in subprocess.check_output(
            [python_config, "--cflags"]
    ).strip().decode('utf-8').split():
        if cflag.startswith('-I'):
            cflags['I'].append(cflag.replace('-I', ''))
        else:
            cflags['extras'].append(cflag)

    for ldflag in subprocess.check_output(
            [python_config, "--ldflags"]
    ).strip().decode('utf-8').split():
        if ldflag.startswith('-l'):
            ldflags['l'].append(ldflag.replace('-l', ''))
        if ldflag.startswith('-L'):
            ldflags['L'].append(ldflag.replace('-L', ''))
        else:
            ldflags['extras'].append(ldflag)

    return {
        'cflags': cflags,
        'ldflags': ldflags
    }


# Disable cythonification if we're not really building anything
if (len(sys.argv) >= 2 and
        any(i in sys.argv[1:] for i in ('--help', 'clean', 'egg_info', '--version')
            )):
    def cythonize(x, **kwargs):
        return x

flags = get_python_flags()

setup(
    name='rados',
    version=__version__,
    description="Python libraries for the Ceph librados library",
    long_description=(
        "This package contains Python libraries for interacting with Ceph's "
        "RADOS library. RADOS is a reliable, autonomic distributed object "
        "storage cluster developed as part of the Ceph distributed storage "
        "system. This is a shared library allowing applications to access "
        "the distributed object store using a simple file-like interface."
    ),
    url='https://github.com/ceph/ceph/tree/master/src/pybind/rados',
    license='LGPLv2+',
    ext_modules=cythonize(
        [
            Extension(
                "rados",
                ["rados.pyx"],
                include_dirs=flags['cflags']['I'],
                library_dirs=flags['ldflags']['L'],
                libraries=["rados"] + flags['ldflags']['l'],
                extra_compile_args=flags['cflags']['extras'] + flags['ldflags']['extras'],
            )
        ], build_dir=os.environ.get("CYTHON_BUILD_DIR", None)
    ),
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Cython',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
)
