from __future__ import print_function

import os
import pkgutil
import shutil
import subprocess
import sys
import tempfile
import textwrap
if not pkgutil.find_loader('setuptools'):
    from distutils.core import setup
    from distutils.extension import Extension
else:
    from setuptools import setup
    from setuptools.extension import Extension
from distutils.ccompiler import new_compiler
from distutils.errors import CompileError, LinkError
from itertools import filterfalse, takewhile
import distutils.sysconfig


def filter_unsupported_flags(compiler, flags):
    args = takewhile(lambda argv: not argv.startswith('-'), [compiler] + flags)
    if any('clang' in arg for arg in args):
        return list(filterfalse(lambda f:
                                f in ('-mcet',
                                      '-fstack-clash-protection',
                                      '-fno-var-tracking-assignments',
                                      '-Wno-deprecated-register',
                                      '-Wno-gnu-designator') or
                                f.startswith('-fcf-protection'),
                                flags))
    else:
        return flags


def monkey_with_compiler(customize):
    def patched(compiler):
        customize(compiler)
        if compiler.compiler_type != 'unix':
            return
        compiler.compiler[1:] = \
            filter_unsupported_flags(compiler.compiler[0],
                                     compiler.compiler[1:])
        compiler.compiler_so[1:] = \
            filter_unsupported_flags(compiler.compiler_so[0],
                                     compiler.compiler_so[1:])
    return patched


distutils.sysconfig.customize_compiler = \
    monkey_with_compiler(distutils.sysconfig.customize_compiler)

# PEP 440 versioning of the Ceph FS package on PyPI
# Bump this version, after every changeset

__version__ = '2.0.0'


def get_python_flags(libs):
    py_libs = sum((libs.split() for libs in
                   distutils.sysconfig.get_config_vars('LIBS', 'SYSLIBS')), [])
    ldflags = list(filterfalse(lambda lib: lib.startswith('-l'), py_libs))
    py_libs = [lib.replace('-l', '') for lib in
               filter(lambda lib: lib.startswith('-l'), py_libs)]
    compiler = new_compiler()
    distutils.sysconfig.customize_compiler(compiler)
    return dict(
        include_dirs=[distutils.sysconfig.get_python_inc()],
        library_dirs=distutils.sysconfig.get_config_vars('LIBDIR', 'LIBPL'),
        libraries=libs + py_libs,
        extra_compile_args=filter_unsupported_flags(
            compiler.compiler[0],
            compiler.compiler[1:] + distutils.sysconfig.get_config_var('CFLAGS').split()),
        extra_link_args=(distutils.sysconfig.get_config_var('LDFLAGS').split() +
                         ldflags))


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

    if 'CEPH_LIBDIR' in os.environ:
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
                **get_python_flags(['cephfs'])
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
        'Programming Language :: Python :: 3',
    ],
    cmdclass=cmdclass,
)
