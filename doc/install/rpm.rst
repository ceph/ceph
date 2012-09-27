========================
 Installing RPM Packages
========================

You may install stable release packages (for stable deployments),
development release packages (for the latest features), or development
testing packages (for development and QA only).  Do not add multiple
package sources at the same time.

Add Stable Release Packages
---------------------------

RPM packages have not been built for the current stable releases, but
are planned for the future.

Add Development Release Packages
--------------------------------

Our development process generates a new release of Ceph every 3-4 weeks.
These packages are faster-moving than the stable releases, as they get
new features integrated quickly, while still undergoing several weeks of QA
prior to release.

Packages are cryptographically signed with the ``release.asc`` key.
Add our release key to your system's list of trusted keys to avoid a
security warning::

    sudo rpm --import https://raw.github.com/ceph/ceph/master/keys/release.asc

Packages are currently built for the Centos6 and Fedora 17 platforms.

The repository package installs the repo details on your local system for yum or up2date to use.

For CentOS6:

    su -c 'rpm -Uvh http://ceph.com/rpms/el6/x86_64/ceph-release-1-0.noarch.rpm'

For Fedora17: 

    su -c 'rpm -Uvh http://ceph.com/rpms/fc17/x86_64/ceph-release-1-0.noarch.rpm'

Installing Packages
-------------------

Once you have added either release or development packages to yum, you
can install Ceph::

	sudo yum install ceph
