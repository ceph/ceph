=============================================================
Configuring the iSCSI Target using the Command Line Interface
=============================================================

The Ceph iSCSI gateway is both an iSCSI target  and a Ceph client;
think of it as a "translator" between Ceph's RBD interface
and the iSCSI standard. The Ceph iSCSI gateway can run on a
standalone node or be colocated with other daemons eg. on
a Ceph Object Store Disk (OSD) node.  When co-locating, ensure
that sufficient CPU and memory are available to share.
The following steps install and configure the Ceph iSCSI gateway for basic operation.

**Requirements:**

-  A running Ceph Luminous or later storage cluster

-  Red Hat Enterprise Linux/CentOS 7.5 (or newer); Linux kernel v4.16 (or newer)

-  The following packages must be installed from your Linux distribution's software repository:

   -  ``targetcli-2.1.fb47`` or newer package

   -  ``python-rtslib-2.1.fb68`` or newer package

   -  ``tcmu-runner-1.4.0`` or newer package

   -  ``ceph-iscsi-3.2`` or newer package

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

#. Install and configure the `Ceph Command-line Interface`_

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

   .. prompt:: bash #

      yum install ceph-iscsi

#. As ``root``, on all iSCSI gateway nodes, install the ``tcmu-runner``
   package:

   .. prompt:: bash #

      yum install tcmu-runner

**Setup:**

#. gwcli requires a pool with the name ``rbd``, so it can store metadata
   like the iSCSI configuration. To check if this pool has been created
   run:

   .. prompt:: bash #

      ceph osd lspools

   If it does not exist instructions for creating pools can be found on the
   `RADOS pool operations page
   <http://docs.ceph.com/en/latest/rados/operations/pools/>`_.

#. As ``root``, on a iSCSI gateway node, create a file named
   ``iscsi-gateway.cfg`` in the ``/etc/ceph/`` directory:

   .. prompt:: bash #

      touch /etc/ceph/iscsi-gateway.cfg

   #. Edit the ``iscsi-gateway.cfg`` file and add the following lines:

      .. code-block:: ini

          [config]
          # Name of the Ceph storage cluster. A suitable Ceph configuration file allowing
          # access to the Ceph storage cluster from the gateway node is required, if not
          # colocated on an OSD node.
          cluster_name = ceph

          # Place a copy of the ceph cluster's admin keyring in the gateway's /etc/ceph
          # directory and reference the filename here
          gateway_keyring = ceph.client.admin.keyring


          # API settings.
          # The API supports a number of options that allow you to tailor it to your
          # local environment. If you want to run the API under https, you will need to
          # create cert/key files that are compatible for each iSCSI gateway node, that is
          # not locked to a specific node. SSL cert and key files *must* be called
          # 'iscsi-gateway.crt' and 'iscsi-gateway.key' and placed in the '/etc/ceph/' directory
          # on *each* gateway node. With the SSL files in place, you can use 'api_secure = true'
          # to switch to https mode.

          # To support the API, the bare minimum settings are:
          api_secure = false

          # Additional API configuration options are as follows, defaults shown.
          # api_user = admin
          # api_password = admin
          # api_port = 5001
          # trusted_ip_list = 192.168.0.10,192.168.0.11

      .. note::
        trusted_ip_list is a list of IP addresses on each iSCSI gateway that
        will be used for management operations like target creation, LUN
        exporting, etc. The IP can be the same that will be used for iSCSI
        data, like READ/WRITE commands to/from the RBD image, but using
        separate IPs is recommended.

      .. important::
        The ``iscsi-gateway.cfg`` file must be identical on all iSCSI gateway nodes.

   #. As ``root``, copy the ``iscsi-gateway.cfg`` file to all iSCSI
      gateway nodes.

#. As ``root``, on all iSCSI gateway nodes, enable and start the API
   service:

   .. prompt:: bash #

      systemctl daemon-reload
       
      systemctl enable rbd-target-gw
      systemctl start rbd-target-gw

      systemctl enable rbd-target-api
      systemctl start rbd-target-api


**Configuring:**

gwcli will create and configure the iSCSI target and RBD images and copy the
configuration across the gateways setup in the last section. Lower level
tools including targetcli and rbd can be used to query the local configuration,
but should not be used to modify it. This next section will demonstrate how
to create a iSCSI target and export a RBD image as LUN 0.

#. As ``root``, on a iSCSI gateway node, start the iSCSI gateway
   command-line interface:

   .. prompt:: bash #

      gwcli

#. Go to iscsi-targets and create a target with the name
   iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw:

   .. code-block:: console

       > /> cd /iscsi-targets
       > /iscsi-targets>  create iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw

#. Create the iSCSI gateways. The IPs used below are the ones that will be
   used for iSCSI data like READ and WRITE commands. They can be the
   same IPs used for management operations listed in trusted_ip_list,
   but it is recommended that different IPs are used.

   .. code-block:: console

       > /iscsi-targets> cd iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways
       > /iscsi-target...-igw/gateways>  create ceph-gw-1 10.172.19.21
       > /iscsi-target...-igw/gateways>  create ceph-gw-2 10.172.19.22

   If not using RHEL/CentOS or using an upstream or ceph-iscsi-test kernel,
   the skipchecks=true argument must be used. This will avoid the Red Hat kernel
   and rpm checks:

   .. code-block:: console

       > /iscsi-targets> cd iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/gateways
       > /iscsi-target...-igw/gateways>  create ceph-gw-1 10.172.19.21 skipchecks=true
       > /iscsi-target...-igw/gateways>  create ceph-gw-2 10.172.19.22 skipchecks=true

#. Add a RBD image with the name disk_1 in the pool rbd:

   .. code-block:: console

       > /iscsi-target...-igw/gateways> cd /disks
       > /disks> create pool=rbd image=disk_1 size=90G

#. Create a client with the initiator name iqn.1994-05.com.redhat:rh7-client:

   .. code-block:: console

       > /disks> cd /iscsi-targets/iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw/hosts
       > /iscsi-target...eph-igw/hosts>  create iqn.1994-05.com.redhat:rh7-client

#. Set the initiator CHAP username and password which the target would
   use when authenticating the initiator:

   .. code-block:: console

       > /iscsi-target...at:rh7-client>  auth username=myusername password=mypassword

   .. warning::
      CHAP must always be configured. Without CHAP, the target will
      reject any login requests.

   To use mutual (bidirectional) authentication, also set the target CHAP
   username and password which the initiator would use when authenticating
   the target:

   .. code-block:: console

       > /iscsi-target...at:rh7-client>  auth username=myusername password=mypassword mutual_username=mytgtusername mutual_password=mytgtpassword

   .. note::
      CHAP usernames must be between 8 and 64 characters long.  Valid
      characters: ``0`` to ``9``, ``a`` to ``z``, ``A`` to ``Z``, ``@``,
      ``_``, ``-``, ``.``, ``:``.

   .. note::
      CHAP passwords must be between 12 and 16 characters long.  Valid
      characters: ``0`` to ``9``, ``a`` to ``z``, ``A`` to ``Z``, ``@``,
      ``_``, ``-``, ``/``.

   .. note::
      For mutual CHAP, initiator and target usernames and passwords
      must not be the same.

#. Add the disk to the client:

   .. code-block:: console

       > /iscsi-target...at:rh7-client> disk add rbd/disk_1

The next step is to configure the iSCSI initiators.

.. _`Ceph Command-line Interface`: ../../start/quick-rbd/#install-ceph

.. toctree::
   :hidden:

   ../../start/quick-rbd
