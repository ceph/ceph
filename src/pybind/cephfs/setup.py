from __future__ import print_function

import os
import pkgutil
import shutil
import subprocess
import sys
import tempfile
import textwrap
from distutils.ccompiler import new_compiler
from distutils.errors import CompileError, LinkError
import distutils.sysconfig

unwrapped_customize = distutils.sysconfig.customize_compiler

clang = False

def filter_unsupported_flags(flags):
    if clang:
        return [f for f in flags if not (f == '-mcet' or
                                         f.startswith('-fcf-protection') or
                                         f == '-fstack-clash-protection')]
    else:
        return flags

def monkey_with_compiler(compiler):
    unwrapped_customize(compiler)
    if compiler.compiler_type == 'unix':
        if compiler.compiler[0].find('clang') != -1:
            global clang
            clang = True
            compiler.compiler = filter_unsupported_flags(compiler.compiler)
            compiler.compiler_so = filter_unsupported_flags(
                compiler.compiler_so)

distutils.sysconfig.customize_compiler = monkey_with_compiler

if not pkgutil.find_loader('setuptools'):
    from distutils.core import setup
    from distutils.extension import Extension
else:
    from setuptools import setup
    from setuptools.extension import Extension

# PEP 440 versioning of the Ceph FS package on PyPI
# Bump this version, after every changeset

__version__ = '2.0.0'


def get_python_flags():
    cflags = {'I': [], 'extras': []}
    ldflags = {'l': [], 'L': [], 'extras': []}

    if os.environ.get('VIRTUAL_ENV', None):
        python = "python"
    else:
        python = 'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)

    python_config = python + '-config'

    for cflag in filter_unsupported_flags(subprocess.check_output(
            [python_config, "--cflags"]).strip().decode('utf-8').split()):
        if cflag.startswith('-I'):
            cflags['I'].append(cflag.replace('-I', ''))
        else:
            cflags['extras'].append(cflag)

    for ldflag in filter_unsupported_flags(subprocess.check_output(
            [python_config, "--ldflags"]).strip().decode('utf-8').split()):
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


def check_sanity():
    """
    Test if development headers and library for cephfs is available by compiling a dummy C program.
    """
    CEPH_SRC_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..',
        '..'
    )

    tmp_dir = tempfile.mkdtemp(dir=os.environ.get('TMPDIR', os.path.dirname(__file__)))
    tmp_file = os.path.join(tmp_dir, 'cephfs_dummy.c')

    with open(tmp_file, 'w') as fp:
        dummy_prog = textwrap.dedent("""
        #include <stddef.h>
        #include "cephfs/libcephfs.h"

        int main(void) {
            struct ceph_mount_info *cmount = NULL;
            ceph_init(cmount);
            return 0;
        }
        """)
        fp.write(dummy_prog)

    compiler = new_compiler()
    distutils.sysconfig.customize_compiler(compiler)

    if {'MAKEFLAGS', 'MFLAGS', 'MAKELEVEL'}.issubset(set(os.environ.keys())):
        # The setup.py has been invoked by a top-level Ceph make.
        # Set the appropriate CFLAGS and LDFLAGS

        compiler.set_library_dirs([os.environ.get('CEPH_LIBDIR')])

    try:
        compiler.define_macro('_FILE_OFFSET_BITS', '64')

        link_objects = compiler.compile(
            sources=[tmp_file],
            output_dir=tmp_dir,
            extra_preargs=['-iquote{path}'.format(path=os.path.join(CEPH_SRC_DIR, 'include'))]
        )

        compiler.link_executable(
            objects=link_objects,
            output_progname=os.path.join(tmp_dir, 'cephfs_dummy'),
            libraries=['cephfs'],
            output_dir=tmp_dir,
        )

    except CompileError:
        print('\nCompile Error: Ceph FS development headers not found', file=sys.stderr)
        return False
    except LinkError:
        print('\nLink Error: Ceph FS library not found', file=sys.stderr)
        return False
    else:
        return True
    finally:
        shutil.rmtree(tmp_dir)


if 'BUILD_DOC' in os.environ.keys():
    pass
elif check_sanity():
    pass
else:
    sys.exit(1)

cmdclass = {}
try:
    from Cython.Build import cythonize
    from Cython.Distutils import build_ext

    cmdclass = {'build_ext': build_ext}
except ImportError:
    print("WARNING: Cython is not installed.")

    if not os.path.isfile('cephfs.c'):
        print('ERROR: Cannot find Cythonized file cephfs.c', file=sys.stderr)
        sys.exit(1)
    else:
        def cythonize(x, **kwargs):
            return x

        source = "cephfs.c"
else:
    source = "cephfs.pyx"

# Disable cythonification if we're not really building anything
if (len(sys.argv) >= 2 and
        any(i in sys.argv[1:] for i in ('--help', 'clean', 'egg_info', '--version')
            )):
    def cythonize(x, **kwargs):
        return x

flags = get_python_flags()

setup(
    name='cephfs',
    version=__version__,
    description="Python bindings for the Ceph FS library",
    long_description=(
        "This package contains Python bindings for interacting with the "
        "Ceph Filesystem (Ceph FS) library. Ceph FS is a POSIX-compliant "
        "filesystem that uses a Ceph Storage Cluster to store its data. The "
        "Ceph filesystem uses the same Ceph Storage Cluster system as "
        "Ceph Block Devices, Ceph Object Storage with its S3 and Swift APIs, "
        "or native bindings (librados)."
    ),
    url='https://github.com/ceph/ceph/tree/master/src/pybind/cephfs',
    license='LGPLv2+',
    platforms='Linux',
    ext_modules=cythonize(
        [
            Extension(
                "cephfs",
                [source],
                include_dirs=flags['cflags']['I'],
                library_dirs=flags['ldflags']['L'],
                libraries=['cephfs'] + flags['ldflags']['l'],
                extra_compile_args=flags['cflags']['extras'] + flags['ldflags']['extras'],
            )
        ],
        compiler_directives={'language_level': sys.version_info.major},
        build_dir=os.environ.get("CYTHON_BUILD_DIR", None),
        include_path=[
            os.path.join(os.path.dirname(__file__), "..", "rados")
        ]
    ),
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Cython',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
    cmdclass=cmdclass,
)
