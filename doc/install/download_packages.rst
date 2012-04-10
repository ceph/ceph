====================
Downloading Packages
====================

We automatically build Debian and Ubuntu packages for any branches or tags that appear in 
the ``ceph.git`` `repository <http://github.com/ceph/ceph>`_. We build packages for the following 
architectures:

- ``amd64`` 
- ``i386`` 

For each architecture, we build packages for the following distributions:

- Debian 7.0 (``wheezy``)
- Debian 6.0 (``squeeze``)
- Debian unstable (``sid``)
- Ubuntu 12.04 (``precise``)
- Ubuntu 11.10 (``oneiric``)
- Ubuntu 11.04 (``natty``)
- Ubuntu 10.10 (``maverick``)

When you execute the following commands to install the Ceph packages, replace ``{ARCH}`` with the architecture of your CPU,
``{DISTRO}`` with the code name of your operating system (e.g., ``wheezy``, rather than the version number) and 
``{BRANCH}`` with the version of Ceph you want to run (e.g., ``master``, ``stable``, ``unstable``, ``v0.44``, etc.). ::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/autobuild.asc \
	| sudo apt-key add -

	sudo tee /etc/apt/sources.list.d/ceph.list <<EOF
	deb http://ceph.newdream.net/debian-snapshot-{ARCH}/{BRANCH}/ {DISTRO} main
	deb-src http://ceph.newdream.net/debian-snapshot-{ARCH}/{BRANCH}/ {DISTRO} main
	EOF

	sudo apt-get update
	sudo apt-get install ceph


When you download packages, you will receive the latest package build, which may be several weeks behind the current release
or the most recent code. It may contain bugs that have already been fixed in the most recent versions of the code. Until packages
contain only stable code, you should carefully consider the tradeoffs of installing from a package or retrieving the latest release
or the most current source code and building Ceph.