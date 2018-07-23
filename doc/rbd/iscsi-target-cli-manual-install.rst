==================================
Manual ceph-iscsi-cli Installation
==================================

**Requirements**

In order to complete the installation of ceph-iscsi-cli, there are 4 different steps:

1. Install common packages from your Linux distribution's software repository
2. Install GIT in order to fetch the remaining packages directly from their GIT repositories
3. Ensure to use a compatible kernel or install a ceph-iscsi-test client
4. Install all the components of ceph-iscsi-cli and start relative daemons:

   -  tcmu-runner
   -  rtslib-fb
   -  configshell-fb
   -  targetcli-fb
   -  ceph-iscsi-config
   -  ceph-iscsi-cli


1. Install Common Packages
==========================

The following packages will be used by ceph-iscsi-cli and target tools.
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
-  python netaddr
-  python netifaces
-  python crypto
-  python requests
-  python flask
-  pyOpenSSL


2. Install GIT
==============

In order to install all the packages needed to run ISCSI with CEPH, you need to download them directly from they repository by using GIT.
On CentOS/RHEL execute:

::

   > sudo yum install git

On Debian/Ubuntu execute:

::

   > sudo apt install git
   
To know more about GIT and how it works please check https://git-scm.com


3. Ensure to use a compatible kernel
====================================

Ensure to use a supported kernel that contains the required Ceph iSCSI patches:

-  all Linux distribution with a kernel v4.16 or newer, or
-  CentOS 7.5 (in this distribution ceph-iscsi support is backported in old kernel)

If you are already using one of this alternative then you can go to next step.
Instead if you NOT using any of those alternative then ceph-client ceph-iscsi-test
branch must be used. To get the branch run:
    
    ::
    
       > git clone https://github.com/ceph/ceph-client.git
       > git checkout ceph-iscsi-test
    
    .. warning::
       ceph-iscsi-test is not for production use. It should only be used
       for proof of concept setups and testing. The kernel is only updated
       with Ceph iSCSI patches. General security and bug fixes from upstream
       are not applied.
    
    Check your distro's docs for specific instructions on how to build a
    kernel. The only Ceph iSCSI specific requirements are the following
    build options must be enabled:
    
    ::
    
       CONFIG_TARGET_CORE=m
       CONFIG_TCM_USER2=m
       CONFIG_ISCSI_TARGET=m


4. Install ceph-iscsi-cli
========================================================

Finally we can start to fetcth all the remaining packages directly from the their GIT repo's and start the relative services.


tcmu-runner
-----------

   Installation:

   ::

       > git clone https://github.com/open-iscsi/tcmu-runner
       > cd tcmu-runner

   Probably you have a fresh minimal install without compilers.
   However developers of 'tcmu-runner' already think about this
   providing a very usefull script to install all the needed compilers.
   Just run to install all the needed dependencies:

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

ceph-iscsi-config
-----------------

   Installation:

   ::

       > git clone https://github.com/ceph/ceph-iscsi-config.git
       > cd ceph-iscsi-config
       > python setup.py install --install-scripts=/usr/bin
       > cp usr/lib/systemd/system/rbd-target-gw.service /lib/systemd/system

   Enable and start the daemon:

   ::

       > systemctl daemon-reload
       > systemctl enable rbd-target-gw
       > systemctl start rbd-target-gw

ceph-iscsi-cli
--------------

   Installation:

   ::

       > git clone https://github.com/ceph/ceph-iscsi-cli.git
       > cd ceph-iscsi-cli
       > python setup.py install --install-scripts=/usr/bin
       > cp usr/lib/systemd/system/rbd-target-api.service /lib/systemd/system


Installation is complete. Proceed to the setup section in the
`main ceph-iscsi-cli page`_.

.. _`main ceph-iscsi-cli page`: ../iscsi-target-cli
