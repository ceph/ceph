import pkgutil
if not pkgutil.find_loader('setuptools'):
    from distutils.core import setup
    from distutils.extension import Extension
else:
    from setuptools import setup
    from setuptools.extension import Extension
import distutils.sysconfig
from distutils.errors import CompileError, LinkError
from distutils.ccompiler import new_compiler
from itertools import filterfalse, takewhile

import os
import shutil
import sys
import tempfile
import textwrap


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

# PEP 440 versioning of the Rados package on PyPI
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
    Test if development headers and library for rados is available by compiling a dummy C program.
    """
    CEPH_SRC_DIR = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..',
        '..'
    )

    tmp_dir = tempfile.mkdtemp(dir=os.environ.get('TMPDIR', os.path.dirname(__file__)))
    tmp_file = os.path.join(tmp_dir, 'rados_dummy.c')

    with open(tmp_file, 'w') as fp:
        dummy_prog = textwrap.dedent("""
        #include <rados/librados.h>

        int main(void) {
            rados_t cluster;
            rados_create(&cluster, NULL);
            return 0;
        }
        """)
        fp.write(dummy_prog)

    compiler = new_compiler()
    distutils.sysconfig.customize_compiler(compiler)

    if 'CEPH_LIBDIR' in os.environ:
        # The setup.py has been invoked by a top-level Ceph make.
        # Set the appropriate CFLAGS and LDFLAGS
        compiler.set_include_dirs([os.path.join(CEPH_SRC_DIR, 'include')])
        compiler.set_library_dirs([os.environ.get('CEPH_LIBDIR')])

    try:
        link_objects = compiler.compile(
            sources=[tmp_file],
            output_dir=tmp_dir
        )
        if ldflags := os.environ.get('LDFLAGS'):
            extra_postargs = ldflags.split()
        else:
            extra_postargs = None
        compiler.link_executable(
            objects=link_objects,
            output_progname=os.path.join(tmp_dir, 'rados_dummy'),
            libraries=['rados'],
            extra_postargs=extra_postargs,
            output_dir=tmp_dir,
        )

    except CompileError:
        print('\nCompile Error: RADOS development headers not found', file=sys.stderr)
        return False
    except LinkError:
        print('\nLink Error: RADOS library not found', file=sys.stderr)
        return False
    else:
        return True
    finally:
        shutil.rmtree(tmp_dir)


if 'BUILD_DOC' in os.environ or 'READTHEDOCS' in os.environ:
    ext_args = {}
    cython_constants = dict(BUILD_DOC=True)
elif check_sanity():
    ext_args = get_python_flags(['rados'])
    cython_constants = dict(BUILD_DOC=False)
else:
    sys.exit(1)

cmdclass = {}
try:
    from Cython.Build import cythonize
    from Cython.Distutils import build_ext

    cmdclass = {'build_ext': build_ext}
except ImportError:
    print("WARNING: Cython is not installed.")

    if not os.path.isfile('rados.c'):
        print('ERROR: Cannot find Cythonized file rados.c', file=sys.stderr)
        sys.exit(1)
    else:
        def cythonize(x, **kwargs):
            return x

        source = "rados.c"
else:
    source = "rados.pyx"

# Disable cythonification if we're not really building anything
if (len(sys.argv) >= 2 and
        any(i in sys.argv[1:] for i in ('--help', 'clean', 'egg_info', '--version')
            )):
    def cythonize(x, **kwargs):
        return x

setup(
    name='rados',
    version=__version__,
    description="Python bindings for the Ceph librados library",
    long_description=(
        "This package contains Python bindings for interacting with Ceph's "
        "RADOS library. RADOS is a reliable, autonomic distributed object "
        "storage cluster developed as part of the Ceph distributed storage "
        "system. This is a shared library allowing applications to access "
        "the distributed object store using a simple file-like interface."
    ),
    url='https://github.com/ceph/ceph/tree/master/src/pybind/rados',
    license='LGPLv2+',
    platforms='Linux',
    ext_modules=cythonize(
        [
            Extension(
                "rados",
                [source],
                **ext_args
            )
        ],
        # use "3str" when Cython 3.0 is available
        compiler_directives={'language_level': sys.version_info.major},
        compile_time_env=cython_constants,
        build_dir=os.environ.get("CYTHON_BUILD_DIR", None),
    ),
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Cython',
        'Programming Language :: Python :: 3'
    ],
    cmdclass=cmdclass,
)
