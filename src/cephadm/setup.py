import setuptools

__version__ = '1.0.0'

setuptools.setup(
     name='cephadm',  
     version=__version__,
     scripts=['cephadm'] ,
     description="cephadm",
     long_description=(
	"The cephadm orchestrator is an orchestrator "
	"module that does not rely on a separate "
	"system such as Rook or Ansible, but rather "
	"manages nodes in a cluster by establishing "
	"an SSH connection and issuing explicit "
	"management commands."
	),
     url="https://github.com/ceph/ceph/tree/master/src/cephadm",
     license='LGPLv2+',
     platforms='Linux',
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)",
         "Operating System :: POSIX :: Linux",
     ],
 )
