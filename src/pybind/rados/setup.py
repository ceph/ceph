# Largely taken from
# https://blog.kevin-brown.com/programming/2014/09/24/combining-autotools-and-setuptools.html
import os, sys, os.path

from setuptools.command.egg_info import egg_info
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

def get_version():
    try:
        for line in open(os.path.join(os.path.dirname(__file__), "..", ".git_version")):
            if "v" in line:
		a=line[1:-1].split("-",2)
		if len(a) > 2:
		  return "{}.post{}+{}".format( a[0], a[1], a[2] )
		elif len(a) > 1:
		  return "{}+{}".format( a[0], a[1] )
		else:
		  return a[0]

                return line[1:-1]
        else:
            return "0"
    except IOError:
        return "0"

class EggInfoCommand(egg_info):
    def finalize_options(self):
        egg_info.finalize_options(self)
        if "build" in self.distribution.command_obj:
            build_command = self.distribution.command_obj["build"]
            self.egg_base = build_command.build_base
            self.egg_info = os.path.join(self.egg_base, os.path.basename(self.egg_info))

# Disable cythonification if we're not really building anything
if (len(sys.argv) >= 2 and
    any(i in sys.argv[1:] for i in ('--help', 'clean', 'egg_info', '--version')
    )):
    def cythonize(x, **kwargs):
        return x

setup(
    name = 'rados',
    version = get_version(),
    description = "Python libraries for the Ceph librados library",
    long_description = (
        "This package contains Python libraries for interacting with Ceph's "
        "rados library."),
    ext_modules = cythonize([
        Extension("rados",
            ["rados.pyx"],
            libraries=["rados"]
            )
    ], build_dir=os.environ.get("CYTHON_BUILD_DIR", None)),
    cmdclass={
        "egg_info": EggInfoCommand,
    },
)
