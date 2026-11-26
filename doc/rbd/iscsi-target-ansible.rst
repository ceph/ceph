==========================================
Configuring the iSCSI Target using Ansible
==========================================

The Ceph iSCSI gateway is the iSCSI target node and also a Ceph client
node. The Ceph iSCSI gateway can be provisioned on dedicated node
or be colocated on a node that runs :term:`Ceph OSD` daemons. The following
steps will install and configure the Ceph iSCSI gateway for basic operation.

**Requirements:**

-  A running Ceph Luminous (12.2.x) cluster or newer

-  Linux kernel v4.16 (or newer), or Enterprise Linux (EL) 7.5 (or
   newer), in which the required iSCSI support is backported

-  The ``ceph-iscsi`` package installed on all the iSCSI gateway nodes

**Installation:**

#. On the Ansible installer node, which could be either the administration node
   or a dedicated deployment node, perform the following steps:

   #. As ``root``, install the ``ceph-ansible`` package:

      ::

          # yum install ceph-ansible

   #. Add an entry in ``/etc/ansible/hosts`` file for the gateway group:

      ::

          [iscsigws]
          ceph-igw-1
          ceph-igw-2

.. note::
  If co-locating the iSCSI gateway with an OSD node, then add the OSD node to the
  ``[iscsigws]`` section.

**Configuration:**

The ``ceph-ansible`` package places a file in the ``/usr/share/ceph-ansible/group_vars/``
directory called ``iscsigws.yml.sample``. Create a copy of this sample file named
``iscsigws.yml``. Review the following Ansible variables and descriptions,
and update accordingly. See the ``iscsigws.yml.sample`` for a full list of
advanced variables.

+--------------------------------------+--------------------------------------+
| Variable                             | Meaning/Purpose                      |
+======================================+======================================+
| ``iscsi_pool_name``                  | Pool that holds the ``gateway.conf`` |
|                                      | configuration object. The default is |
|                                      | ``rbd``.                             |
+--------------------------------------+--------------------------------------+
| ``copy_admin_key``                   | If ``True`` (the default), copy the  |
|                                      | ``client.admin`` keyring to each     |
|                                      | gateway.                             |
+--------------------------------------+--------------------------------------+
| ``generate_crt``                     | If ``True``, generate a self-signed  |
|                                      | certificate for the API. The default |
|                                      | is ``False``.                        |
+--------------------------------------+--------------------------------------+
| ``api_user``                         | The user name for the API. The       |
|                                      | default is `admin`.                  |
+--------------------------------------+--------------------------------------+
| ``api_password``                     | The password for using the API. The  |
|                                      | default is `admin`.                  |
+--------------------------------------+--------------------------------------+
| ``api_port``                         | The TCP port number for using the    |
|                                      | API. The default is `5000`.          |
+--------------------------------------+--------------------------------------+
| ``api_secure``                       | True if TLS must be used. The        |
|                                      | default is `false`. If true the user |
|                                      | must create the necessary            |
|                                      | certificate and key files. See the   |
|                                      | gwcli man file for details.          |
+--------------------------------------+--------------------------------------+
| ``trusted_ip_list``                  | A list of IPv4 or IPv6 addresses     |
|                                      | who have access to the API. By       |
|                                      | default, only the iSCSI gateway      |
|                                      | nodes have access.                   |
+--------------------------------------+--------------------------------------+

**Deployment:**

Perform the following steps on the Ansible installer node.

#. As ``root``, execute the Ansible playbook:

   .. prompt:: bash #

      cd /usr/share/ceph-ansible
      ansible-playbook site.yml --limit iscsigws

   .. note::
    The Ansible playbook will handle RPM dependencies, setting up daemons,
    and installing gwcli so it can be used to create iSCSI targets and export
    RBD images as LUNs. In past versions, ``iscsigws.yml`` could define the
    iSCSI target and other objects like clients, images and LUNs, but this is
    no longer supported.

#. Verify the configuration from an iSCSI gateway node:

   .. prompt:: bash #

      gwcli ls

   .. note::
    See the `Configuring the iSCSI Target using the Command Line Interface`_
    section to create gateways, LUNs, and clients using the `gwcli` tool.

   .. important::
    Attempting to use the ``targetcli`` tool to change the configuration will
    cause problems including ALUA misconfiguration and path failover
    issues. There is the potential to corrupt data, to have mismatched
    configuration across iSCSI gateways, and to have mismatched WWN information,
    leading to client multipath problems.

**Service Management:**

The ``ceph-iscsi`` package installs the configuration management
logic and a Systemd service called ``rbd-target-api``. When the Systemd
service is enabled, the ``rbd-target-api`` will start at boot time and
will restore the Linux-IO (LIO) state. The Ansible playbook disables the
target service during the deployment. Below are the outcomes of when
interacting with the ``rbd-target-api`` Systemd service.

.. prompt:: bash #

   systemctl <start|stop|restart|reload> rbd-target-api

-  ``reload``

   A reload request will force ``rbd-target-api`` to reread the
   configuration and apply it to the current running environment. This
   is normally not required, since changes are deployed in parallel from
   Ansible to all iSCSI gateway nodes

-  ``stop``

   A stop request will close the gateway’s portal interfaces, dropping
   connections to clients and wipe the current LIO configuration from
   the kernel. This returns the iSCSI gateway to a clean state. When
   clients are disconnected, active I/O is rescheduled to the other
   iSCSI gateways by the client side multipathing layer.

**Removing the Configuration:**

The ``ceph-ansible`` package provides an Ansible playbook to
remove the iSCSI gateway configuration and related RBD images. The
Ansible playbook is
``/usr/share/ceph-ansible/infrastructure-playbooks/purge-iscsi-gateways.yml``.
When this Ansible playbook is ran a prompted for the type of purge to
perform:

*lio* :

In this mode the LIO configuration is purged on all iSCSI gateways that
are defined. Disks that were created are left untouched within the Ceph
storage cluster.

*all* :

When ``all`` is chosen, the LIO configuration is removed together with
**all** RBD images that were defined within the iSCSI gateway
environment, other unrelated RBD images will not be removed. Ensure the
correct mode is chosen, this operation will delete data.

.. warning::
  A purge operation is destructive action against your iSCSI gateway
  environment.

.. warning::
  A purge operation will fail, if RBD images have snapshots or clones
  and are exported through the Ceph iSCSI gateway.

.. highlight:: console

::

    [root@rh7-iscsi-client ceph-ansible]# ansible-playbook infrastructure-playbooks/purge-iscsi-gateways.yml
    Which configuration elements should be purged? (all, lio or abort) [abort]: all


    PLAY [Confirm removal of the iSCSI gateway configuration] *********************


    GATHERING FACTS ***************************************************************
    ok: [localhost]


    TASK: [Exit playbook if user aborted the purge] *******************************
    skipping: [localhost]


    TASK: [set_fact ] *************************************************************
    ok: [localhost]


    PLAY [Removing the gateway configuration] *************************************


    GATHERING FACTS ***************************************************************
    ok: [ceph-igw-1]
    ok: [ceph-igw-2]


    TASK: [igw_purge | purging the gateway configuration] *************************
    changed: [ceph-igw-1]
    changed: [ceph-igw-2]


    TASK: [igw_purge | deleting configured rbd devices] ***************************
    changed: [ceph-igw-1]
    changed: [ceph-igw-2]


    PLAY RECAP ********************************************************************
    ceph-igw-1                 : ok=3    changed=2    unreachable=0    failed=0
    ceph-igw-2                 : ok=3    changed=2    unreachable=0    failed=0
    localhost                  : ok=2    changed=0    unreachable=0    failed=0


.. _Configuring the iSCSI Target using the Command Line Interface: ../iscsi-target-cli
