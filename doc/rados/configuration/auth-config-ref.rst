.. _rados-cephx-config-ref:

========================
 CephX Config Reference
========================

The CephX protocol is enabled by default. The cryptographic authentication that
CephX provides has some computational costs, though they should generally be
quite low. If the network environment connecting your client and server hosts
is very safe and you cannot afford authentication, you can disable it.
**Disabling authentication is not generally recommended**.

.. note:: If you disable authentication, you will be at risk of a
   man-in-the-middle attack that alters your client/server messages, which
   could have disastrous security effects.

For information about creating users, see `User Management`_. For details on
the architecture of CephX, see `Architecture - High Availability
Authentication`_.


Deployment Scenarios
====================

How you initially configure CephX depends on your scenario. There are two
common strategies for deploying a Ceph cluster.  If you are a first-time Ceph
user, you should probably take the easiest approach: using ``cephadm`` to
deploy a cluster. But if your cluster uses other deployment tools (for example,
Ansible, Chef, Juju, or Puppet), you will need either to use the manual
deployment procedures or to configure your deployment tool so that it will
bootstrap your monitor(s).

Manual Deployment
-----------------

When you deploy a cluster manually, it is necessary to bootstrap the monitors
manually and to create the ``client.admin`` user and keyring. To bootstrap
monitors, follow the steps in `Monitor Bootstrapping`_. Follow these steps when
using third-party deployment tools (for example, Chef, Puppet, and Juju).


Enabling/Disabling CephX
========================

Enabling CephX is possible only if the keys for your monitors, OSDs, and
metadata servers have already been deployed. If you are simply toggling CephX
on or off, it is not necessary to repeat the bootstrapping procedures.

Enabling CephX
--------------

When CephX is enabled, Ceph will look for the keyring in the default search
path: this path includes ``/etc/ceph/$cluster.$name.keyring``. It is possible
to override this search-path location by adding a ``keyring`` option in the
``[global]`` section of your `Ceph configuration`_ file, but this is not
recommended.

To enable CephX on a cluster for which authentication has been disabled, carry
out the following procedure.  If you (or your deployment utility) have already
generated the keys, you may skip the steps related to generating keys.

#. Create a ``client.admin`` key, and save a copy of the key for your client
   host:

   .. prompt:: bash $

     ceph auth get-or-create client.admin mon 'allow *' mds 'allow *' mgr 'allow *' osd 'allow *' -o /etc/ceph/ceph.client.admin.keyring

   **Warning:** This step will clobber any existing
   ``/etc/ceph/client.admin.keyring`` file. Do not perform this step if a
   deployment tool has already generated a keyring file for you. Be careful!

#. Create a monitor keyring and generate a monitor secret key:

   .. prompt:: bash $

     ceph-authtool --create-keyring /tmp/ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'

#. For each monitor, copy the monitor keyring into a ``ceph.mon.keyring`` file
   in the monitor's ``mon data`` directory. For example, to copy the monitor
   keyring to ``mon.a`` in a cluster called ``ceph``, run the following
   command:

   .. prompt:: bash $

     cp /tmp/ceph.mon.keyring /var/lib/ceph/mon/ceph-a/keyring

#. Generate a secret key for every MGR, where ``{$id}`` is the MGR letter:

   .. prompt:: bash $

      ceph auth get-or-create mgr.{$id} mon 'allow profile mgr' mds 'allow *' osd 'allow *' -o /var/lib/ceph/mgr/ceph-{$id}/keyring

#. Generate a secret key for every OSD, where ``{$id}`` is the OSD number:

   .. prompt:: bash $

      ceph auth get-or-create osd.{$id} mon 'allow rwx' osd 'allow *' -o /var/lib/ceph/osd/ceph-{$id}/keyring

#. Generate a secret key for every MDS, where ``{$id}`` is the MDS letter:

   .. prompt:: bash $

      ceph auth get-or-create mds.{$id} mon 'allow rwx' osd 'allow *' mds 'allow *' mgr 'allow profile mds' -o /var/lib/ceph/mds/ceph-{$id}/keyring

#. Enable CephX authentication by setting the following options in the
   ``[global]`` section of your `Ceph configuration`_ file:

   .. code-block:: ini

      auth_cluster_required = cephx
      auth_service_required = cephx
      auth_client_required = cephx

#. Start or restart the Ceph cluster. For details, see `Operating a Cluster`_.

For details on bootstrapping a monitor manually, see `Manual Deployment`_.



Disabling CephX
---------------

The following procedure describes how to disable CephX. If your cluster
environment is safe, you might want to disable CephX in order to offset the
computational expense of running authentication. **We do not recommend doing
so.** However, setup and troubleshooting might be easier if authentication is
temporarily disabled and subsequently re-enabled.

#. Disable CephX authentication by setting the following options in the
   ``[global]`` section of your `Ceph configuration`_ file:

   .. code-block:: ini

      auth_cluster_required = none
      auth_service_required = none
      auth_client_required = none

#. Start or restart the Ceph cluster. For details, see `Operating a Cluster`_.


Configuration Settings
======================

Enablement
----------


``auth_cluster_required``

:Description: If this configuration setting is enabled, the Ceph Storage
              Cluster daemons (that is, ``ceph-mon``, ``ceph-osd``,
              ``ceph-mds``, and ``ceph-mgr``) are required to authenticate with
              each other. Valid settings are ``cephx`` or ``none``.

:Type: String
:Required: No
:Default: ``cephx``.


``auth_service_required``

:Description: If this configuration setting is enabled, then Ceph clients can
              access Ceph services only if those clients authenticate with the
              Ceph Storage Cluster.  Valid settings are ``cephx`` or ``none``.

:Type: String
:Required: No
:Default: ``cephx``.


``auth_client_required``

:Description: If this configuration setting is enabled, then communication
              between the Ceph client and Ceph Storage Cluster can be
              established only if the Ceph Storage Cluster authenticates
              against the Ceph client. Valid settings are ``cephx`` or
              ``none``.

:Type: String
:Required: No
:Default: ``cephx``.


.. index:: keys; keyring

Keys
----

When Ceph is run with authentication enabled, ``ceph`` administrative commands
and Ceph clients can access the Ceph Storage Cluster only if they use
authentication keys.

The most common way to make these keys available to ``ceph`` administrative
commands and Ceph clients is to include a Ceph keyring under the ``/etc/ceph``
directory. For Octopus and later releases that use ``cephadm``, the filename is
usually ``ceph.client.admin.keyring``.  If the keyring is included in the
``/etc/ceph`` directory, then it is unnecessary to specify a ``keyring`` entry
in the Ceph configuration file.

Because the Ceph Storage Cluster's keyring file contains the ``client.admin``
key, we recommend copying the keyring file to nodes from which you run
administrative commands.

To perform this step manually, run the following command:

.. prompt:: bash $

   sudo scp {user}@{ceph-cluster-host}:/etc/ceph/ceph.client.admin.keyring /etc/ceph/ceph.client.admin.keyring

.. tip:: Make sure that the ``ceph.keyring`` file has appropriate permissions
   (for example, ``chmod 644``) set on your client machine.

You can specify the key itself by using the ``key`` setting in the Ceph
configuration file (this approach is not recommended), or instead specify a
path to a keyfile by using the ``keyfile`` setting in the Ceph configuration
file.

``keyring``

:Description: The path to the keyring file.
:Type: String
:Required: No
:Default: ``/etc/ceph/$cluster.$name.keyring,/etc/ceph/$cluster.keyring,/etc/ceph/keyring,/etc/ceph/keyring.bin``


``keyfile``

:Description: The path to a keyfile (that is, a file containing only the key).
:Type: String
:Required: No
:Default: None


``key``

:Description: The key (that is, the text string of the key itself). We do not
              recommend that you use this setting unless you know what you're
              doing.
:Type: String
:Required: No
:Default: None


Daemon Keyrings
---------------

Administrative users or deployment tools (for example, ``cephadm``) generate
daemon keyrings in the same way that they generate user keyrings. By default,
Ceph stores the keyring of a daemon inside that daemon's data directory. The
default keyring locations and the capabilities that are necessary for the
daemon to function are shown below.

``ceph-mon``

:Location: ``$mon_data/keyring``
:Capabilities: ``mon 'allow *'``

``ceph-osd``

:Location: ``$osd_data/keyring``
:Capabilities: ``mgr 'allow profile osd' mon 'allow profile osd' osd 'allow *'``

``ceph-mds``

:Location: ``$mds_data/keyring``
:Capabilities: ``mds 'allow' mgr 'allow profile mds' mon 'allow profile mds' osd 'allow rwx'``

``ceph-mgr``

:Location: ``$mgr_data/keyring``
:Capabilities: ``mon 'allow profile mgr' mds 'allow *' osd 'allow *'``

``radosgw``

:Location: ``$rgw_data/keyring``
:Capabilities: ``mon 'allow rwx' osd 'allow rwx'``


.. note:: The monitor keyring (that is, ``mon.``) contains a key but no
   capabilities, and this keyring is not part of the cluster ``auth`` database.

The daemon's data-directory locations default to directories of the form::

  /var/lib/ceph/$type/$cluster-$id

For example, ``osd.12`` would have the following data directory::

  /var/lib/ceph/osd/ceph-12

It is possible to override these locations, but it is not recommended.


.. index:: signatures

Signatures
----------

Ceph performs a signature check that provides some limited protection against
messages being tampered with in flight (for example, by a "man in the middle"
attack).

As with other parts of Ceph authentication, signatures admit of fine-grained
control.  You can enable or disable signatures for service messages between
clients and Ceph, and for messages between Ceph daemons.

Note that even when signatures are enabled data is not encrypted in flight.

``cephx_require_signatures``

:Description: If this configuration setting is set to ``true``, Ceph requires
              signatures on all message traffic between the Ceph client and the
              Ceph Storage Cluster, and between daemons within the Ceph Storage
              Cluster.

.. note::
          **ANTIQUATED NOTE:**

          Neither Ceph Argonaut nor Linux kernel versions prior to 3.19
          support signatures; if one of these clients is in use, ``cephx_require_signatures``
          can be disabled in order to allow the client to connect.


:Type: Boolean
:Required: No
:Default: ``false``


``cephx_cluster_require_signatures``

:Description: If this configuration setting is set to ``true``, Ceph requires
              signatures on all message traffic between Ceph daemons within the
              Ceph Storage Cluster.

:Type: Boolean
:Required: No
:Default: ``false``


``cephx_service_require_signatures``

:Description: If this configuration setting is set to ``true``, Ceph requires
              signatures on all message traffic between Ceph clients and the
              Ceph Storage Cluster.

:Type: Boolean
:Required: No
:Default: ``false``


``cephx_sign_messages``

:Description: If this configuration setting is set to ``true``, and if the Ceph
              version supports message signing, then Ceph will sign all
              messages so that they are more difficult to spoof.

:Type: Boolean
:Default: ``true``


Time to Live
------------

``auth_service_ticket_ttl``

:Description: When the Ceph Storage Cluster sends a ticket for authentication
              to a Ceph client, the Ceph Storage Cluster assigns that ticket a
              Time To Live (TTL).

:Type: Double
:Default: ``60*60``


.. _Monitor Bootstrapping: ../../../install/manual-deployment#monitor-bootstrapping
.. _Operating a Cluster: ../../operations/operating
.. _Manual Deployment: ../../../install/manual-deployment
.. _Ceph configuration: ../ceph-conf
.. _Architecture - High Availability Authentication: ../../../architecture#high-availability-authentication
.. _User Management: ../../operations/user-management
