======================
 5-minute Quick Start
======================

Thank you for trying Ceph! Petabyte-scale data clusters are quite an 
undertaking. Before delving deeper into Ceph, we recommend setting up a
cluster on a single host to explore some of the functionality. 

Ceph **5-Minute Quick Start** is intended for use on one machine with a 
recent Debian/Ubuntu operating system. The intent is to help you exercise 
Ceph functionality without the deployment overhead associated with a 
production-ready storage cluster.

Install Debian/Ubuntu
---------------------

Install a recent release of Debian or Ubuntu (e.g., 12.04 precise). 

Add Ceph Packages
-----------------

To get the latest Ceph packages, add a release key to APT, add a source
location to your ``/etc/apt/sources.list``, update your system and 
install Ceph. :: 

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc | sudo apt-key add -	
	echo deb http://ceph.com/debian/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list
	sudo apt-get update && sudo apt-get install ceph
	
Add a Configuration File
------------------------

Modify the contents of the following configuration file such that
``localhost`` is the actual host name, and the monitor IP address 
is the actual IP address of the host (i.e., not 127.0.0.1).Then, 
copy the contents of the modified configuration file and save it to
``/etc/ceph/ceph.conf``. This file will configure Ceph to operate a monitor, 
two OSD daemons and one metadata server on your local machine. 

.. literalinclude:: ceph.conf
   :language: ini
   
Deploy the Configuration
------------------------
To deploy the configuration, create a directory for each daemon as follows::

	sudo mkdir /var/lib/ceph/osd/ceph-0
	sudo mkdir /var/lib/ceph/osd/ceph-1
	sudo mkdir /var/lib/ceph/mon/ceph-a
	sudo mkdir /var/lib/ceph/mds/ceph-a

	cd /etc/ceph
	sudo mkcephfs -a -c /etc/ceph/ceph.conf -k ceph.keyring

Start the Ceph Cluster
----------------------

Once you have deployed the configuration, start the Ceph cluster. :: 

	sudo service ceph start
	
Check the health of your Ceph cluster to ensure it is ready. :: 

	ceph health
	
If your cluster echoes back ``HEALTH_OK``, you may begin using your cluster.