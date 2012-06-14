======================
 Installing OpenStack
======================

Installing OpenStack with DevStack
----------------------------------

To install OpenStack with `DevStack`_, you should ensure that your 
packages are up to date and properly upgraded. 

.. tip:: For Ubuntu 12.04 installations, ensure that you updgrade
   your distribution to the latest release. 
   
For example:: 

	sudo apt-get update && sudo apt-get upgrade && sudo apt-get dist-upgrade
	
Once you have completed the updates, reboot your system. 

Clone the DevStack repository and install OpenStack. ::

	git clone git://github.com/openstack-dev/devstack.git
	cd devstack; ./stack.sh

The installer will prompt you to enter passwords for the various
components. Follow the installer to take appropriate notes.

.. _DevStack: http://devstack.org/

Installing OpenStack with Chef
------------------------------
Coming Soon!

Installing OpenStack with Crowbar
---------------------------------
Coming Soon!