====================
Host Recommendations
====================
We recommend using a dedicated Administration host for larger deployments, particularly when you intend to build Ceph from source
or build your own packages. 

.. important:: The Administration host must have ``root`` access to OSD Cluster hosts for installation and maintenance.

If you are installing Ceph on a single host for the first time to learn more about it, your local host is effectively your Administration host.

Enable Extended Attributes (XATTRs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you are using a file system other than ``XFS`` or ``btrfs``, you need to enable extended attributes. ::

	mount -t ext4 -o user_xattr /dev/hda mount/mount_point 

Install Build Prerequisites
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Install `build prerequisites`_ on your Administration host machine. If you want to 
build install packages on each OSD Cluster host, install the `build prerequisites`_
on each OSD Cluster host.

Configure SSH
~~~~~~~~~~~~~
Before you can install and configure Ceph on an OSD Cluster host you need to configure SSH on 
the Administration host and on each OSD Cluster host. 

You must be able to login via SSH from your Administration host to each OSD Cluster host 
as ``root`` using the short name to identify the OSD Cluster host. Your Administration host must be able 
to connect to each OSD Cluster host using the OSD Cluster host's short name, not its full domain name (e.g., ``shortname`` 
not ``shortname.domain.com``).

To connect to an OSD Cluster host from your Administration host using the OSD Cluster short name only, 
add a host configuration for the OSD Cluster host to your ``~/.ssh_config file``. You must add an entry 
for each host in your cluster. Set the user name to ``root`` or a username with root privileges. For example:: 

	Host shortname1
		Hostname shortname1.domain.com
		User root
	Host shortname2
		Hostname shortname2.domain.com
		User root

You must be able to use ``sudo`` over SSH from the Administration host on each OSD Cluster host
without ``sudo`` prompting you for a password.

If you have a public key for your root password in ``.ssh/id_rsa.pub``, you must copy the key and append it
to the contents of the ``.ssh/authorized_keys`` file on each OSD Cluster host. Create the ``.ssh/authorized_keys``
file if it doesn't exist. When you open an SSH connection from the Administration host to an OSD Cluster host, 
SSH uses the private key in the home directory of the Administration host and tries to match it with a public
key in the home directory of the OSD Cluster host. If the SSH keys match, SSH will log you in automatically. 
If the SSH keys do not match, SSH will prompt you for a password. 

To generate an SSH key pair on your Administration host, log in with ``root`` permissions, go to your ``/home`` directory and enter the following::

	$ ssh-keygen

Add Packages to OSD Cluster Hosts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Add the packages you downloaded or built on the Administration host to each OSD Cluster host. Perform the same steps 
from `Installing Debian/Ubuntu Packages`_ for each OSD Cluster host. To expedite adding 
the ``etc/apt/sources.list.d/ceph.list`` file to each OSD Cluster host, consider using ``tee``. 
::

	$ sudo tee /etc/apt/sources.list.d/ceph.list <<EOF

A prompt will appear, and you can add lines to the ``ceph.list`` file. For release packages, enter::

	> deb http://ceph.newdream.net/debian/{BRANCH}/ {DISTRO} main
	
For snapshot packages, enter:: 

	> deb http://ceph.newdream.net/debian-snapshot-amd64/{BRANCH}/ {DISTRO} main
	> deb-src http://ceph.newdream.net/debian-snapshot-amd64/{BRANCH}/ {DISTRO} main  	

For packages you built on your Administration host, consider making them accessible via HTTP, and enter:: 

	> deb http://{adminhostname}.domainname.com/{package directory}
	
Once you have added the package directories, close the file. :: 

	> EOF


.. _build prerequisites: ../build_from_source/build_prerequisites
.. _Installing Debian/Ubuntu Packages: ../download_packages