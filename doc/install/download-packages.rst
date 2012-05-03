====================================
 Downloading Debian/Ubuntu Packages
====================================
We automatically build Debian/Ubuntu packages for any branches or tags that
appear in the ``ceph.git`` `repository <http://github.com/ceph/ceph>`_. If you
want to build your own packages (*e.g.,* for RPM), see
`Build Ceph Packages <../../source/build-packages>`_.

When you download release packages, you will receive the latest package build,
which may be several weeks behind the current release or the most recent code.
It may contain bugs that have already been fixed in the most recent versions of
the code. Until packages contain only stable code, you should carefully consider
the tradeoffs of installing from a package or retrieving the latest release
or the most current source code and building Ceph.

When you execute the following commands to install the Debian/Ubuntu Ceph
packages, replace ``{ARCH}`` with the architecture of your CPU (*e.g.,* ``amd64``
or ``i386``), ``{DISTRO}`` with the code name of your operating system
(*e.g.,* ``precise``, rather than the OS version number) and ``{BRANCH}`` with
the version of Ceph you want to run (e.g., ``master``, ``stable``, ``unstable``,
``v0.44``, *etc.*).

Adding Release Packages to APT
------------------------------
We provide stable release packages for Debian/Ubuntu, which are signed signed
with the ``release.asc`` key. Click `here <http://ceph.newdream.net/debian/dists>`_
to see the distributions and branches supported. To install a release package,
you must first add a release key. ::

	$ wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc \ | sudo apt-key add -

For Debian/Ubuntu releases, we use the Advanced Package Tool (APT). To retrieve
the release packages and updates and install them with ``apt``, you must add a
``ceph.list`` file to your ``apt`` configuration with the following path::

	etc/apt/sources.list.d/ceph.list

Open the file and add the following line::

	deb http://ceph.com/debian/ {DISTRO} main

Remember to replace ``{DISTRO}`` with the Linux distribution for your host.
Then, save the file.

Downloading Packages
--------------------
Once you add either release or autobuild packages for Debian/Ubuntu, you may
download them with ``apt`` as follows::

	sudo apt-get update
