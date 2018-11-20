==============================
Manual ceph-iscsi Installation
==============================

**Requirements**

To complete the installation of ceph-iscsi, there are 4 steps:

1. Install common packages from your Linux distribution's software repository
2. Install Git to fetch the remaining packages directly from their Git repositories
3. Ensure a compatible kernel is used
4. Install all the components of ceph-iscsi and start associated daemons:

   -  tcmu-runner
   -  rtslib-fb
   -  configshell-fb
   -  targetcli-fb
   -  ceph-iscsi


1. Install Common Packages
==========================

The following packages will be used by ceph-iscsi and target tools.
They must be installed from your Linux distribution's software repository
on each machine that will be a iSCSI gateway:

-  libnl3
-  libkmod
-  librbd1
-  pyparsing
-  python kmod
-  python pyudev
-  python gobject
-  python urwid
-  python pyparsing
-  python rados
-  python rbd
-  python netifaces
-  python crypto
-  python requests
-  python flask
-  pyOpenSSL


2. Install Git
==============

In order to install all the packages needed to run iSCSI with Ceph, you need to download them directly from their repository by using Git.
On CentOS/RHEL execute:

::

   > sudo yum install git

On Debian/Ubuntu execute:

::

   > sudo apt install git
   
To know more about Git and how it works, please, visit https://git-scm.com


3. Ensure a compatible kernel is used
=====================================

Ensure you use a supported kernel that contains the required Ceph iSCSI patches:

-  all Linux distribution with a kernel v4.16 or newer, or
-  Red Hat Enterprise Linux or CentOS 7.5 or later (in these distributions ceph-iscsi support is backported)

If you are already using a compatible kernel, you can go to next step.
However, if you are NOT using a compatible kernel then check your distro's
documentation for specific instructions on how to build this kernel. The only
Ceph iSCSI specific requirements are that the following build options must be
enabled:

    ::
    
       CONFIG_TARGET_CORE=m
       CONFIG_TCM_USER2=m
       CONFIG_ISCSI_TARGET=m


4. Install ceph-iscsi
========================================================

Finally, the remaining tools can be fetched directly from their Git repositories and their associated services started


tcmu-runner
-----------

   Installation:

   ::

       > git clone https://github.com/open-iscsi/tcmu-runner
       > cd tcmu-runner

   Run the following command to install all the needed dependencies:

   ::

       > ./extra/install_dep.sh   
   
   Now you can build the tcmu-runner.
   To do so, use the following build command:

   ::

       > cmake -Dwith-glfs=false -Dwith-qcow=false -DSUPPORT_SYSTEMD=ON -DCMAKE_INSTALL_PREFIX=/usr
       > make install

   Enable and start the daemon:

   ::

       > systemctl daemon-reload
       > systemctl enable tcmu-runner
       > systemctl start tcmu-runner


rtslib-fb
---------

   Installation:

   ::

       > git clone https://github.com/open-iscsi/rtslib-fb.git
       > cd rtslib-fb
       > python setup.py install

configshell-fb
--------------

   Installation:

   ::

       > git clone https://github.com/open-iscsi/configshell-fb.git
       > cd configshell-fb
       > python setup.py install

targetcli-fb
------------

   Installation:

   ::

       > git clone https://github.com/open-iscsi/targetcli-fb.git
       > cd targetcli-fb
       > python setup.py install
       > mkdir /etc/target
       > mkdir /var/target

   .. warning:: The ceph-iscsi tools assume they are managing all targets
      on the system. If targets have been setup and are being managed by
      targetcli the target service must be disabled.

ceph-iscsi
-----------------

   Installation:

   ::

       > git clone https://github.com/ceph/ceph-iscsi.git
       > cd ceph-iscsi
       > python setup.py install --install-scripts=/usr/bin
       > cp usr/lib/systemd/system/rbd-target-gw.service /lib/systemd/system
       > cp usr/lib/systemd/system/rbd-target-api.service /lib/systemd/system

   Enable and start the daemon:

   ::

       > systemctl daemon-reload
       > systemctl enable rbd-target-gw
       > systemctl start rbd-target-gw
       > systemctl enable rbd-target-api
       > systemctl start rbd-target-api

Installation is complete. Proceed to the setup section in the
`main ceph-iscsi CLI page`_.

.. _`main ceph-iscsi CLI page`: ../iscsi-target-cli
