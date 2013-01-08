========================
 Installing RPM Packages
========================

You may install stable release packages (for stable deployments),
development release packages (for the latest features), or development
testing packages (for development and QA only).  Do not add multiple
package sources at the same time.

Install Release Key
===================

Packages are cryptographically signed with the ``release.asc`` key. Add our
release key to your system's list of trusted keys to avoid a security warning::

    sudo rpm --import https://raw.github.com/ceph/ceph/master/keys/release.asc

Add Release Packages
====================

Bobtail
-------

Bobtail is the most recent major release of Ceph.  These packages are
recommended for anyone deploying Ceph in a production environment.
Critical bug fixes are backported and point releases are made as necessary.

Packages are currently built for the RHEL/CentOS6 (``el6``), Fedora 17
(``f17``), OpenSUSE 12 (``opensuse12``), and SLES (``sles11``)
platforms. The repository package installs the repository details on
your local system for use with ``yum`` or ``up2date``.

Replase the``{DISTRO}`` below with the distro codename::

    su -c 'rpm -Uvh http://ceph.com/rpm-bobtail/{DISTRO}/x86_64/ceph-release-1-0.el6.noarch.rpm'

For example, for CentOS 6 or other RHEL6 derivatives (``el6``)::

    su -c 'rpm -Uvh http://ceph.com/rpm-bobtail/el6/x86_64/ceph-release-1-0.el6.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-bobtail


Development Release Packages
----------------------------

Our development process generates a new release of Ceph every 3-4 weeks. These
packages are faster-moving than the stable releases. Development packages have
new features integrated quickly, while still undergoing several weeks of QA
prior to release.

Packages are cryptographically signed with the ``release.asc`` key. Add our
release key to your system's list of trusted keys to avoid a security warning::

    sudo rpm --import https://raw.github.com/ceph/ceph/master/keys/release.asc

Packages are currently built for the CentOS-6 and Fedora 17 platforms. The
repository package installs the repository details on your local system for use
with ``yum`` or ``up2date``.

For CentOS-6::

    su -c 'rpm -Uvh http://ceph.com/rpms/el6/x86_64/ceph-release-1-0.el6.noarch.rpm'

For Fedora 17:: 

    su -c 'rpm -Uvh http://ceph.com/rpms/fc17/x86_64/ceph-release-1-0.fc17.noarch.rpm'

You can download the RPMs directly from::

     http://ceph.com/rpm-testing

Installing Packages
===================

Once you have added either release or development packages to ``yum``, you
can install Ceph::

	sudo yum install ceph
