=============================================================
Configuring the iSCSI Target using the Command Line Interface
=============================================================

The Ceph iSCSI gateway is the iSCSI target node and also a Ceph client
node. The Ceph iSCSI gateway can be a standalone node or be colocated on
a Ceph Object Store Disk (OSD) node. Completing the following steps will
install, and configure the Ceph iSCSI gateway for basic operation.

**Requirements:**

-  A running Ceph Luminous or later storage cluster

-  Red Hat Enterprise Linux/CentOS 7.5 (or newer); Linux kernel v4.16 (or newer)

-  The following packages must be installed from your Linux distribution's software repository:

   -  ``targetcli-2.1.fb47`` or newer package

   -  ``python-rtslib-2.1.fb64`` or newer package

   -  ``tcmu-runner-1.3.0`` or newer package

   -  ``ceph-iscsi-2.7`` or newer package

     .. important::
        If previous versions of these packages exist, then they must
        be removed first before installing the newer versions.

Do the following steps on the Ceph iSCSI gateway node before proceeding
to the *Installing* section:

#. If the Ceph iSCSI gateway is not colocated on an OSD node, then copy
   the Ceph configuration files, located in ``/etc/ceph/``, from a
   running Ceph node in the storage cluster to the iSCSI Gateway node.
   The Ceph configuration files must exist on the iSCSI gateway node
   under ``/etc/ceph/``.

#. Install and configure the `Ceph Command-line
   Interface <http://docs.ceph.com/docs/master/start/quick-rbd/#install-ceph>`_

#. If needed, open TCP ports 3260 and 5000 on the firewall.

   .. note::
      Access to port 5000 should be restricted to a trusted internal network or
      only the individual hosts where ``gwcli`` is used or ``ceph-mgr`` daemons
      are running.

#. Create a new or use an existing RADOS Block Device (RBD).

**Installing:**

If you are using the upstream ceph-iscsi package follow the
`manual install instructions`_.

.. _`manual install instructions`: ../iscsi-target-cli-manual-install

.. toctree::
   :hidden:

   iscsi-target-cli-manual-install

For rpm based instructions execute the following commands:

#. As ``root``, on all iSCSI gateway nodes, install the
   ``ceph-iscsi`` package:

   ::

       # yum install ceph-iscsi

#. As ``root``, on all iSCSI gateway nodes, install the ``tcmu-runner``
   package:

   ::

       # yum install tcmu-runner

**Setup:**

#. gwcli requires a pool with the name ``rbd``, so it can store metadata
   like the iSCSI configuration. To check if this pool has been created
   run:

   ::

       # ceph osd lspools

   If it does not exist instructions for creating pools can be found on the
   `RADOS pool operations page
   <http://docs.ceph.com/docs/master/rados/operations/pools/>`_.

#. As ``root``, on a iSCSI gateway node, create a file named
   ``iscsi-gateway.cfg`` in the ``/etc/ceph/`` directory:

   ::

       # touch /etc/ceph/iscsi-gateway.cfg

   #. Edit the ``iscsi-gateway.cfg`` file and add the following lines:

      ::

          [config]
          # Name of the Ceph storage cluster. A suitable Ceph configuration file allowing
          # access to the Ceph storage cluster from the gateway node is required, if not
          # colocated on an OSD node.
          cluster_name = ceph

          # Place a copy of the ceph cluster's admin keyring in the gateway's /etc/ceph
          # drectory and reference the filename here
          gateway_keyring = ceph.client.admin.keyring


          # API settings.
          # The API supports a number of options that allow you to tailor it to your
          # local environment. If you want to run the API under https, you will need to
          # create cert/key files that are compatible for each iSCSI gateway node, that is
          # not locked to a specific node. SSL cert and key files *must* be called
          # 'iscsi-gateway.crt' and 'iscsi-gateway.key' and placed in the '/etc/ceph/' directory
          # on *each* gateway node. With the SSL files in place, you can use 'api_secure = true'
          # to switch to https mode.

          # To support the API, the bear minimum settings are:
          api_secure = false

          # Additional API configuration options are as follows, defaults shown.
          # api_user = admin
          # api_password = admin
          # api_port = 5001
          # trusted_ip_list = 192.168.0.10,192.168.0.11

      .. note::
        trusted_ip_list is a list of IP addresses on each iscsi gateway that
        will be used for management operations like target creation, lun
        exporting, etc. The IP can be the same that will be used for iSCSI
        data, like READ/WRITE commands to/from the RBD image, but using
        separate IPs is recommended.

      .. important::
        The ``iscsi-gateway.cfg`` file must be identical on all iSCSI gateway nodes.

   #. As ``root``, copy the ``iscsi-gateway.cfg`` file to all iSCSI
      gateway nodes.

#. As ``root``, on all iSCSI gateway nodes, enable and start the API
   service:

   ::

       # systemctl daemon-reload
       # systemctl enable rbd-target-api
       # systemctl start rbd-target-api

**Configuring:**

gwcli will create and configure the iSCSI target and RBD images and copy the
configuration across the gateways setup in the last section. Lower level
tools, like targetcli and rbd, can be used to query the local configuration,
but should not be used to modify it. This next section will demonstrate how
to create a iSCSI target and export a RBD image as LUN 0.

#. As ``root``, on a iSCSI gateway node, start the iSCSI gateway
   command-line interface:

   ::

       # gwcli

#. Go to iscsi-targets and create a target with the name
   iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw:

   ::

       > /> cd /iscsi-target
       > /iscsi-target>  create iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw

#. Create the iSCSI gateways. The IPs used below are the ones that will be
   used for iSCSI data like READ and WRITE commands. They can be the
   same IPs used for management operations listed in trusted_ip_list,
   but it is recommended that different IPs are used.

   ::

       > /iscsi-target> cd iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways
       > /iscsi-target...-igw/gateways>  create ceph-gw-1 10.172.19.21
       > /iscsi-target...-igw/gateways>  create ceph-gw-2 10.172.19.22

   If not using RHEL/CentOS or using an upstream or ceph-iscsi-test kernel,
   the skipchecks=true argument must be used. This will avoid the Red Hat kernel
   and rpm checks:

   ::

       > /iscsi-target> cd iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways
       > /iscsi-target...-igw/gateways>  create ceph-gw-1 10.172.19.21 skipchecks=true
       > /iscsi-target...-igw/gateways>  create ceph-gw-2 10.172.19.22 skipchecks=true

#. Add a RBD image with the name disk_1 in the pool rbd:

   ::

       > /iscsi-target...-igw/gateways> cd /disks
       > /disks> create pool=rbd image=disk_1 size=90G

#. Create a client with the initiator name iqn.1994-05.com.redhat:rh7-client:

   ::

       > /disks> cd /iscsi-target/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts
       > /iscsi-target...eph-igw/hosts>  create iqn.1994-05.com.redhat:rh7-client

#. Set the client's CHAP username to myiscsiusername and password to
   myiscsipassword:

   ::

       > /iscsi-target...at:rh7-client>  auth username=myiscsiusername password=myiscsipassword

   .. warning::
      CHAP must always be configured. Without CHAP, the target will
      reject any login requests.

#. Add the disk to the client:

   ::

       > /iscsi-target...at:rh7-client> disk add rbd/disk_1

The next step is to configure the iSCSI initiators.
