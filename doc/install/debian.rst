===================================
 Installing Debian/Ubuntu Packages
===================================

We build Debian and Ubuntu packages for each stable release of Ceph
which are signed with the ``release.asc`` key. See `the Debian
repository <http://ceph.newdream.net/debian/dists>`_ for a full list
of distributions supported.

First, add our release key to your system's list of trusted keys to
avoid a security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc \ | sudo apt-key add -

Next, add our package repository to your system's list of APT sources::

        sudo bash -c "echo deb http://ceph.com/debian/ {DISTRO} main > /etc/apt/sources.list.d/ceph.list"

Remember to replace ``{DISTRO}`` with the code name for Debian/Ubuntu
distribution (*e.g.,* ``precise`` for Ubuntu ``12.04``) and ``{ARCH}``
with the architecture of your CPU (``amd64`` for 64-bit systems and
``i386`` for 32-bit systems).

Once you configure the source for either release or autobuild packages
for Debian/Ubuntu, you need to update APT's database as follows::

	sudo apt-get update

To install Ceph::

        sudo apt-get install ceph

