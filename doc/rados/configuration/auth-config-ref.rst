.. _rados-cephx-config-ref:

========================
 Cephx Config Reference
========================

The ``cephx`` protocol is enabled by default. Cryptographic authentication has
some computational costs, though they should generally be quite low.  If the
network environment connecting your client and server hosts is very safe and
you cannot afford authentication, you can turn it off. **This is not generally
recommended**.

.. note:: If you disable authentication, you are at risk of a man-in-the-middle
   attack altering your client/server messages, which could lead to disastrous
   security effects.

For creating users, see `User Management`_. For details on the architecture
of Cephx, see `Architecture - High Availability Authentication`_.


Deployment Scenarios
====================

There are two main scenarios for deploying a Ceph cluster, which impact
how you initially configure Cephx. Most first time Ceph users use
``cephadm`` to create a cluster (easiest). For clusters using
other deployment tools (e.g., Chef, Juju, Puppet, etc.), you will need
to use the manual procedures or configure your deployment tool to
bootstrap your monitor(s).

Manual Deployment
-----------------

When you deploy a cluster manually, you have to bootstrap the monitor manually
and create the ``client.admin`` user and keyring. To bootstrap monitors, follow
the steps in `Monitor Bootstrapping`_. The steps for monitor bootstrapping are
the logical steps you must perform when using third party deployment tools like
Chef, Puppet,  Juju, etc.


Enabling/Disabling Cephx
========================

Enabling Cephx requires that you have deployed keys for your monitors,
OSDs and metadata servers. If you are simply toggling Cephx on / off,
you do not have to repeat the bootstrapping procedures.


Enabling Cephx
--------------

When ``cephx`` is enabled, Ceph will look for the keyring in the default search
path, which includes ``/etc/ceph/$cluster.$name.keyring``. You can override
this location by adding a ``keyring`` option in the ``[global]`` section of
your `Ceph configuration`_ file, but this is not recommended.

Execute the following procedures to enable ``cephx`` on a cluster with
authentication disabled. If you (or your deployment utility) have already
generated the keys, you may skip the steps related to generating keys.

#. Create a ``client.admin`` key, and save a copy of the key for your client
   host

   .. prompt:: bash $

     ceph auth get-or-create client.admin mon 'allow *' mds 'allow *' mgr 'allow *' osd 'allow *' -o /etc/ceph/ceph.client.admin.keyring

   **Warning:** This will clobber any existing
   ``/etc/ceph/client.admin.keyring`` file. Do not perform this step if a
   deployment tool has already done it for you. Be careful!

#. Create a keyring for your monitor cluster and generate a monitor
   secret key.

   .. prompt:: bash $

     ceph-authtool --create-keyring /tmp/ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'

#. Copy the monitor keyring into a ``ceph.mon.keyring`` file in every monitor's
   ``mon data`` directory. For example, to copy it to ``mon.a`` in cluster ``ceph``,
   use the following

   .. prompt:: bash $

     cp /tmp/ceph.mon.keyring /var/lib/ceph/mon/ceph-a/keyring

#. Generate a secret key for every MGR, where ``{$id}`` is the MGR letter

   .. prompt:: bash $

      ceph auth get-or-create mgr.{$id} mon 'allow profile mgr' mds 'allow *' osd 'allow *' -o /var/lib/ceph/mgr/ceph-{$id}/keyring

#. Generate a secret key for every OSD, where ``{$id}`` is the OSD number

   .. prompt:: bash $

      ceph auth get-or-create osd.{$id} mon 'allow rwx' osd 'allow *' -o /var/lib/ceph/osd/ceph-{$id}/keyring

#. Generate a secret key for every MDS, where ``{$id}`` is the MDS letter

   .. prompt:: bash $

      ceph auth get-or-create mds.{$id} mon 'allow rwx' osd 'allow *' mds 'allow *' mgr 'allow profile mds' -o /var/lib/ceph/mds/ceph-{$id}/keyring

#. Enable ``cephx`` authentication by setting the following options in the
   ``[global]`` section of your `Ceph configuration`_ file

   .. code-block:: ini

      auth_cluster_required = cephx
      auth_service_required = cephx
      auth_client_required = cephx


#. Start or restart the Ceph cluster. See `Operating a Cluster`_ for details.

For details on bootstrapping a monitor manually, see `Manual Deployment`_.



Disabling Cephx
---------------

The following procedure describes how to disable Cephx. If your cluster
environment is relatively safe, you can offset the computation expense of
running authentication. **We do not recommend it.** However, it may be easier
during setup and/or troubleshooting to temporarily disable authentication.

#. Disable ``cephx`` authentication by setting the following options in the
   ``[global]`` section of your `Ceph configuration`_ file

   .. code-block:: ini

      auth_cluster_required = none
      auth_service_required = none
      auth_client_required = none


#. Start or restart the Ceph cluster. See `Operating a Cluster`_ for details.


Configuration Settings
======================

Enablement
----------


.. confval:: auth_cluster_required
.. confval:: auth_service_required
.. confval:: auth_client_required

.. index:: keys; keyring

Keys
----

When you run Ceph with authentication enabled, ``ceph`` administrative commands
and Ceph Clients require authentication keys to access the Ceph Storage Cluster.

The most common way to provide these keys to the ``ceph`` administrative
commands and clients is to include a Ceph keyring under the ``/etc/ceph``
directory. For Octopus and later releases using ``cephadm``, the filename
is usually ``ceph.client.admin.keyring`` (or ``$cluster.client.admin.keyring``).
If you include the keyring under the ``/etc/ceph`` directory, you don't need to
specify a ``keyring`` entry in your Ceph configuration file.

We recommend copying the Ceph Storage Cluster's keyring file to nodes where you
will run administrative commands, because it contains the ``client.admin`` key.

To perform this step manually, execute the following:

.. prompt:: bash $

   sudo scp {user}@{ceph-cluster-host}:/etc/ceph/ceph.client.admin.keyring /etc/ceph/ceph.client.admin.keyring

.. tip:: Ensure the ``ceph.keyring`` file has appropriate permissions set
   (e.g., ``chmod 644``) on your client machine.

You may specify the key itself in the Ceph configuration file using the ``key``
setting (not recommended), or a path to a keyfile using the ``keyfile`` setting.

.. confval:: keyring
   :default: /etc/ceph/$cluster.$name.keyring,/etc/ceph/$cluster.keyring,/etc/ceph/keyring,/etc/ceph/keyring.bin
.. confval:: keyfile
.. confval:: key

.. index:: signatures

Signatures
----------

Ceph performs a signature check that provides some limited protection
against messages being tampered with in flight (e.g., by a "man in the
middle" attack).

Like other parts of Ceph authentication, Ceph provides fine-grained control so
you can enable/disable signatures for service messages between clients and
Ceph, and so you can enable/disable signatures for messages between Ceph daemons.

Note that even with signatures enabled data is not encrypted in
flight.

.. confval:: cephx_require_signatures
.. confval:: cephx_cluster_require_signatures
.. confval:: cephx_service_require_signatures
.. confval:: cephx_sign_messages

Time to Live
------------

.. confval:: auth_service_ticket_ttl

.. _Monitor Bootstrapping: ../../../install/manual-deployment#monitor-bootstrapping
.. _Operating a Cluster: ../../operations/operating
.. _Manual Deployment: ../../../install/manual-deployment
.. _Ceph configuration: ../ceph-conf
.. _Architecture - High Availability Authentication: ../../../architecture#high-availability-authentication
.. _User Management: ../../operations/user-management
