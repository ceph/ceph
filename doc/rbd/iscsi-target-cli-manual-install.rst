==================================
Manual ceph-iscsi-cli Installation
==================================

**Requirements**

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

**Packages**

-  Linux Kernel

   If not using a distro kernel that contains the required Ceph iSCSI patches,
   then Linux kernel v4.16 or newer or the ceph-client ceph-iscsi-test
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

-  tcmu-runner

   Installation:

   ::

       > git clone https://github.com/open-iscsi/tcmu-runner
       > cd tcmu-runner

   Ceph iSCSI requires systemd so the following build command must be used:

   ::

       > cmake -Dwith-glfs=false -Dwith-qcow=false -DSUPPORT_SYSTEMD=ON -DCMAKE_INSTALL_PREFIX=/usr
       > make install

   Enable and start the daemon:

   ::

       > systemctl daemon-reload
       > systemctl enable tcmu-runner
       > systemctl start tcmu-runner

-  rtslib-fb

   Installation:

   ::

       > git clone https://github.com/open-iscsi/rtslib-fb.git
       > cd rtslib-fb
       > python setup.py install

-  configshell-fb

   Installation:

   ::

       > git clone https://github.com/open-iscsi/configshell-fb.git
       > cd configshell-fb
       > python setup.py install

-  targetcli-fb

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

-  ceph-iscsi-config

   Installation:

   ::

       > git clone https://github.com/ceph/ceph-iscsi-config.git
       > cd ceph-iscsi-config
       > python setup.py install
       > cp usr/lib/systemd/system/rbd-target-gw.service /lib/systemd/system

   Enable and start the daemon:

   ::

       > systemctl daemon-reload
       > systemctl enable rbd-target-gw
       > systemctl start rbd-target-gw

-  ceph-iscsi-cli

   Installation:

   ::

       > git clone https://github.com/ceph/ceph-iscsi-cli.git
       > cd ceph-iscsi-cli
       > python setup.py install
       > cp usr/lib/systemd/system/rbd-target-api.service /lib/systemd/system


Installation is complete. Proceed to the setup section in the
`main ceph-iscsi-cli page`_.

.. _`main ceph-iscsi-cli page`: ../iscsi-target-cli
