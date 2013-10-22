==============
 Get Packages
==============

If you are attempting to install behind a firewall in an environment without internet 
access, you must retrieve the packages (mirrored with all the necessary dependencies) 
before attempting an install.

Debian Packages
===============

Ceph requires additional additional third party libraries.  

- libaio1
- libsnappy1
- libcurl3
- curl
- libgoogle-perftools4
- google-perftools
- libleveldb1


The repository package installs the repository details on your local system for
use with ``apt``. Replace ``{release}`` with the latest Ceph release. Replace
``{version}`` with the latest Ceph version number. Replace ``{distro}`` with
your Linux distribution codename. Replace ``{arch}`` with the CPU architecture.

::

	wget -q http://ceph.com/debian-{release}/pool/main/c/ceph/ceph_{version}{distro}_{arch}.deb


RPM Packages
============

Ceph requires additional additional third party libraries.  
To add the EPEL repository, execute the following:: 

   su -c 'rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm'

Ceph requires the following packages:

- snappy
- leveldb
- gdisk
- python-argparse
- gperftools-libs


Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 18 and 19
(``f18`` and ``f19``), OpenSUSE 12.2 (``opensuse12.2``), and SLES (``sles11``)
platforms. The repository package installs the repository details on your local
system for use with ``yum`` or ``up2date``. Replace ``{distro}`` with your
distribution. ::

    su -c 'rpm -Uvh http://ceph.com/rpm-emperor/{distro}/noarch/ceph-{version}.{distro}.noarch.rpm'

For example, for CentOS 6  (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-emperor/el6/noarch/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

	http://ceph.com/rpm-emperor


For earlier Ceph releases, replace ``{release-name}`` with the name 
with the name of the Ceph release. You may call ``lsb_release -sc`` on the command 
line to get the short codename. ::

	su -c 'rpm -Uvh http://ceph.com/rpm-{release-name}/{distro}/noarch/ceph-{version}.{distro}.noarch.rpm'

