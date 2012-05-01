========================
Installing Ceph Packages
========================
Once you have downloaded or built Ceph packages, you may install them on your Admin host and OSD Cluster hosts.


.. important:: All hosts should be running the same package version. 
   To ensure that you are running the same version on each host with APT, you may 
   execute ``sudo apt-get update`` on each host before you install the packages.


Installing Packages with APT
----------------------------
Once you download or build the packages and add your packages to APT 
(see `Downloading Debian/Ubuntu Packages <../download_packages>`_), you may install them as follows::

	$ sudo apt-get install ceph


Installing Packages with RPM
----------------------------
Once you have built your RPM packages, you may install them as follows::

	rpm -i rpmbuild/RPMS/x86_64/ceph-*.rpm


Proceed to Creating a Cluster
-----------------------------
Once you have prepared your hosts and installed Ceph pages, proceed to `Creating a Storage Cluster <../../create_cluster>`_. 