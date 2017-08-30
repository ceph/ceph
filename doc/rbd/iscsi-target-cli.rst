=============================================================
Configuring the iSCSI Target using the Command Line Interface
=============================================================

The Ceph iSCSI gateway is the iSCSI target node and also a Ceph client
node. The Ceph iSCSI gateway can be a standalone node or be colocated on
a Ceph Object Store Disk (OSD) node. Completing the following steps will
install, and configure the Ceph iSCSI gateway for basic operation.

**Requirements:**

-  A running Ceph Luminous or later storage cluster

-  RHEL/CentOS 7.4; or Linux kernel v4.14 or newer

-  The following packages must be installed from your Linux distribution's software repository:

   -  ``targetcli-2.1.fb47`` or newer package

   -  ``python-rtslib-2.1.fb64`` or newer package

   -  ``tcmu-runner-1.3.0`` or newer package

   -  ``ceph-iscsi-config-2.3`` or newer package

   -  ``ceph-iscsi-cli-2.5`` or newer package

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

#. Create a new or use an existing RADOS Block Device (RBD).

**Installing:**

#. As ``root``, on all iSCSI gateway nodes, install the
   ``ceph-iscsi-cli`` package:

   ::

       # yum install ceph-iscsi-cli

#. As ``root``, on all iSCSI gateway nodes, install the ``tcmu-runner``
   package:

   ::

       # yum install tcmu-runner

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

      .. important::
        The ``iscsi-gateway.cfg`` file must be identical on all iSCSI gateway nodes.

   #. As ``root``, copy the ``iscsi-gateway.cfg`` file to all iSCSI
      gateway nodes.

#. As ``root``, on all iSCSI gateway nodes, enable and start the API
   service:

   ::

       # systemctl enable rbd-target-api
       # systemctl start rbd-target-api

**Configuring:**

#. As ``root``, on a iSCSI gateway node, start the iSCSI gateway
   command-line interface:

   ::

       # gwcli

#. Creating the iSCSI gateways:

   ::

       >/iscsi-target create iqn.2003-01.com.redhat.iscsi-gw:<target_name>
       > goto gateways
       > create <iscsi_gw_name> <IP_addr_of_gw>
       > create <iscsi_gw_name> <IP_addr_of_gw>

#. Adding a RADOS Block Device (RBD):

   ::

       > cd /iscsi-target/iqn.2003-01.com.redhat.iscsi-gw:<target_name>/disks/
       >/disks/ create pool=<pool_name> image=<image_name> size=<image_size>m|g|t

#. Creating a client:

   ::

       > goto hosts
       > create iqn.1994-05.com.redhat:<client_name>
       > auth chap=<user_name>/<password> | nochap


  .. warning::
      CHAP must always be configured. Without CHAP, the target will
      reject any login requests.

#. Adding disks to a client:

   ::

       >/iscsi-target..eph-igw/hosts> cd iqn.1994-05.com.redhat:<client_name>
       > disk add <pool_name>.<image_name>

The next step is to configure the iSCSI initiators.
