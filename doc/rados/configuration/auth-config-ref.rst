.. _rados-cephx-config-ref:

========================
 CephX Config Reference
========================

The CephX protocol is enabled by default. The cryptographic authentication that
CephX provides has some computational costs, though they should generally be
quite low. If the network environment connecting your client and server hosts
is very safe and you cannot afford authentication, you can disable it.

.. warning:: Disabling authentication is usually a very bad choice. If you
             disable authentication, any access to the cluster will be
             permitted no matter the origin or identity of the client.

For information about creating users, see :ref:`user-management`. For details on
the architecture of CephX, see :ref:`arch_high_availability_authentication`.


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

When you deploy a cluster manually, it is necessary to bootstrap the Monitors
manually and to create the ``client.admin`` user and keyring. To bootstrap
Monitors, follow the steps in `Monitor Bootstrapping`_. Follow these steps when
using third-party deployment tools (for example, Chef, Puppet, and Juju).


Enabling/Disabling CephX
========================

Enabling CephX is possible only if the keys for your Monitors, OSD, and MDS
have already been deployed. If you are simply toggling CephX on or off, it is
not necessary to repeat the bootstrapping procedures.

Authentication is explicitly enabled or disabled for all entities via the
``global`` section of Ceph configuration. The following configurations affect
this.

.. confval::  auth_cluster_required
.. confval::  auth_service_required
.. confval::  auth_client_required


Enabling CephX
--------------

When CephX is enabled, Ceph will look for the keyring in the default search
path: this path includes ``/etc/ceph/$cluster.$name.keyring``. It is possible
to override this search path location by adding a ``keyring`` option in the
``[global]`` section of your :ref:`Ceph configuration <configuring-ceph>`
file, but this is not recommended.

To enable CephX on a cluster for which authentication has been disabled, carry
out the following procedure.  If you (or your deployment utility) have already
generated the keys, you may skip the steps related to generating keys.

#. Create a ``client.admin`` key, and save a copy of the key for your client
   host:

.. prompt:: bash $

    ceph auth get-or-create client.admin mon 'allow *' mds 'allow *' mgr 'allow *' osd 'allow *' -o /etc/ceph/ceph.client.admin.keyring

.. warning:: This step will clobber any existing ``/etc/ceph/client.admin.keyring`` file. Do not perform this step if a deployment tool has already generated a keyring file for you. Be careful!

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
   ``[global]`` section of your :ref:`Ceph configuration <configuring-ceph>`
   file:

   .. code-block:: ini

      [global]
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
   ``[global]`` section of your :ref:`Ceph configuration <configuring-ceph>`
   file:

   .. code-block:: ini

      [global]
      auth_cluster_required = none
      auth_service_required = none
      auth_client_required = none

#. Start or restart the Ceph cluster. For details, see `Operating a Cluster`_.


Configuration Settings
======================

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
in the :ref:`Ceph configuration <configuring-ceph>` file.

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

.. confval::  keyring
.. confval::  keyfile
.. confval::  key


Daemon Keyrings
---------------

Administrative users or deployment tools (for example, ``cephadm``) generate
daemon keyrings in the same way that they generate user keyrings. By default,
Ceph stores the keyring of a daemon inside that daemon's data directory.
Consult each components documentation for capabilities expected for the
service.

To bootstrap a cluster, consult the :ref:`manual-deployment` documentation.

Each daemon's data-directory locations defaults to a path of the form::

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

Note that even when signatures are enabled, data is not encrypted in flight.

.. confval:: cephx_require_signatures

.. confval:: cephx_cluster_require_signatures

.. confval:: cephx_service_require_signatures

.. confval:: cephx_sign_messages


Time to Live
------------

.. confval:: auth_mon_ticket_ttl
.. confval:: auth_service_ticket_ttl



.. _cephx-upgrade:

Upgrading and Rotating CephX Keys
=================================

In 2026, it became necessary to upgrade the cipher key type for all CephX keys
due to the potential vulnerabilities in the older encryption schemes. To effect
this upgrade, it's necessary to do the upgrade in several steps.

.. note:: cephadm and Rook automate this process for you. cephadm however does not handle the client key changes.


#. **Allow the newer key types for authentication.** For upgraded clusters, this should be done automatically by the Monitors.

   Confirm the upgrade:

   .. code:: bash

       ceph --format=json mon dump | jq -r '.auth_allowed_ciphers | map(.name) | join (",")'

   should output something like:

   ::

       aes,aes256k

   where ``aes256k`` is the new more secure cipher type.

   If not included, you can explicitly enable it:

   .. code:: bash

       ceph mon set auth_allowed_ciphers aes,aes256k

   Then confirm the change:

   .. code:: bash

       ceph --format=json mon dump | jq -r '.auth_allowed_ciphers | map(.name) | join (",")'


#. **Set the preferred default cipher type for new keys.** You may choose **not** to do this if you want new keys, by default, to use the older cipher type until your client applications can be upgraded.

   Check the current value:

   .. code:: bash

       ceph --format=json mon dump | jq -r '.auth_preferred_cipher.name'

   might output:

   ::

       aes

   To upgrade to ``aes256k`` as the new default cipher type, execute:

   .. code:: bash

       ceph mon set auth_preferred_cipher aes256k

   Confirm the change:

   .. code:: bash

       ceph --format=json mon dump | jq -r '.auth_preferred_cipher.name'

   should output:

   ::

       aes256k


#. **Rotate the keys for all service daemon credentials.** These include **mon**, **mgr**, **osd**, and **mds**.

   .. warning:: Changing the key will make the existing daemon unable to reauthenticate.

   .. note:: The ``mon.`` historically has not been managed by the Monitor auth database; it exists soley in each Monitor's keyring inside its data directory. This suggested rotation procedure now puts the authoritative copy in the auth database alongside other keys. The Monitor keyring persists as a fallback or emergency key.

   Begin with the ``mon.`` key:

   .. code:: bash

       ceph auth rotate --key-type=aes256k mon. | tee mon.keyring

   Save the ``mon.keyring`` file in a safe place. It should **not** be necessary to update the keyring files for each Monitor.

   Restart each Monitor:

   .. code:: bash

       systemctl restart ceph-mon@$ID

   .. warning:: If a Monitor was out-of-quorum during the Monitor key rotation, it will not have the new key. You must put the saved ``mon.keyring`` in its keyring file so it can authenticate.


   Now, for each other service daemon type (**mgr**, **osd**, and **mds**):

   Stop the daemon:

   .. code:: bash

       systemctl stop ceph-$TYPE@$ID

   If it is an OSD:

   .. code:: bash

       ceph osd down $ID

   Rotate the entity's key:

   .. code:: bash

       ceph auth rotate --key-type=aes256k $TYPE.$ID | tee keyring

   .. note:: If you have updated ``auth_preferred_cipher`` then you can omit ``--key-type``.

   Copy the ``keyring`` file to the machine running the daemon, then execute:

   .. code:: bash

       ceph-authtool --import-keyring $COPIED_KEYRING /var/lib/ceph/$TYPE/ceph-$ID/keyring

   Adjust the above script based on where the data directory for your daemons are located.

   Finally, restart the daemon:

   .. code:: bash

       systemctl restart ceph-$TYPE@$ID

#. **Confirm the** :ref:`auth-insecure-service-key-type` **is cleared.**

   .. code:: bash

       ceph --format=json health detail | jq '.checks | has("AUTH_INSECURE_SERVICE_KEY_TYPE") | not'

   output gives ``false``.

   If it outputs ``true``, there is another daemon that needs to be upgraded.
   Check the output of ``ceph health detail``.

#. **Upgrade the cipher for rotating service keys.**

   .. code:: bash

       ceph mon set auth_service_cipher aes256k

   Confirm the change:

   .. code:: bash

       ceph --format=json mon dump | jq -r '.auth_service_cipher.name'

   should output

   ::

       aes256k

   Verify the :ref:`auth-insecure-service-tickets` is resolved:

   .. code:: bash

       ceph --format=json health detail | jq '.checks | has("AUTH_INSECURE_SERVICE_TICKETS") | not'

#. **Wipe the rotating service keys.**

   .. warning:: This **is not recommended** for most deployments. It is best to let your rotating service keys expire after a few hours (using default TTL).

   .. warning:: **Only perform this step if all service daemons have upgraded binaries that understand the new cipher type.**

   If you want to immediately clear the :ref:`auth-insecure-rotating-service-key-type` warning, you can wipe the existing rotating service key database on the Monitors:

   .. code::

       ceph auth wipe-rotating-service-keys

   should output:

   ::

       wiped rotating service keys!

   This will cause all service daemons to refresh the rotating service keys. Upgraded clients will similarly refresh their tickets with the Monitors.

   .. note:: This operation has no effect on the existing sessions the Clients have established with service daemons.

#. **Prevent creation of new insecure keys.**

   When the Monitor setting ``auth_allowed_ciphers`` setting includes an
   insecure key type, the **default value** of the Monitor config
   ``mon_auth_allow_insecure_key`` will be altered at runtime to ``true``. For an upgraded
   cluster, you should therefore expect see the
   :ref:`auth-insecure-keys-creatable` health warning.

   You can disable this configuration manually to prevent new insecure keys
   from being created. Alternatively, once the ``auth_allowed_ciphers`` omits
   insecure key types (in a future step of this process), this configuration
   will have its default value changed and that should also clear
   ``AUTH_INSECURE_KEYS_CREATABLE``.

   To manually disable the creation of insecure keys:

   .. code:: bash

       ceph config set mon 'mon auth allow insecure key' false

   Verify the ``AUTH_INSECURE_KEYS_CREATABLE`` is resolved:

   .. code:: bash

       ceph --format=json health detail | jq '.checks | has("AUTH_INSECURE_KEYS_CREATABLE") | not'

   output gives ``false``.

   For more information, see :ref:`auth_allow_insecure_keys`.

#. **Rotate the admin key.**

   .. warning:: Rotating the admin key requires special care as recovering from a mistake is complicated. Be careful.

   .. note:: It is common for the ``client.admin`` credential's key to be copied to several nodes that may need to execute administrative commands. The new key will need to be copied to each node.

   Create a backup emergency admin key in case of mistakes:

   .. code:: bash

        ceph auth get-or-create client.admin-backup mon "allow *" | tee ./client.admin-backup.keyring

   Confirm the backup key works:

   .. code:: bash

        ceph -n client.admin-backup -k ./client.admin-backup.keyring auth ls

   Now, rotate the ``client.admin`` key:

   .. code:: bash

       ceph auth rotate --key-type=aes256k client.admin | tee ./client.admin.keyring

   .. note:: If you have updated ``auth_preferred_cipher`` then you can omit ``--key-type``.

   .. warning:: The client.admin key is now changed. You cannot execute new Ceph commands as ``client.admin`` until you import the new key into your keyring.

   Import the new ``client.admin`` key into your system's keyring file:

   .. code:: bash

       ceph-authtool --import-keyring ./client.admin.keyring /etc/ceph/ceph.client.admin.keyring

   .. warning:: Your system's keyring file may be in a different location! Check ``/etc/ceph`` and your local Ceph configuration.

   Verify the key works:

   .. code:: bash

       ceph -n client.admin -k /etc/ceph/ceph.client.admin.keyring ceph auth ls

   If everything looks good, remove the backup key:

   .. code:: bash

       ceph auth rm client.admin-backup

#. **Rotate other client keys.**

   The process to rotate other client keys is similar to the admin key.

   To view ``client`` credentials with insecure keys:

   .. code:: bash

       ceph health detail

   should include output with :ref:`auth-insecure-client-key-type`:

   ::

       [WRN] AUTH_INSECURE_CLIENT_KEY_TYPE: 2 auth client entities with insecure key types
        entity client.fs using insecure key type: aes
        entity client.fs_a using insecure key type: aes

   which tells you that two keys need to be updated.

   If the client's software (e.g. ``ceph-fuse`` or the Linux kernel driver) is
   up-to-date on all machines using the key, you may rotate the key and
   distribute it.

   .. code:: bash

       ceph auth rotate --key-type=aes256k client.$ID | tee ./client.$ID.keyring

   .. note:: If you have updated ``auth_preferred_cipher`` then you can omit ``--key-type``.

   Then copy and import the key to each machine using that ``client.$ID`` credential.

   Once all client credentials have been upgraded, you should see the ``AUTH_INSECURE_CLIENT_KEY_TYPE`` health warning clear.

   .. code:: bash

       ceph --format=json health detail | jq '.checks | has("AUTH_INSECURE_CLIENT_KEY_TYPE") | not'

   output gives ``false``.

   If you cannot rotate a particular client key yet, you may prefer to mute the
   health warning until you can complete upgrading all of the client keys. We
   expect this to be typical situation for some clusters.

   .. code::

       ceph health mute AUTH_INSECURE_CLIENT_KEY_TYPE 8w

   to mute the warning for 8 weeks. Alternatively, use ``--sticky`` to make it permanent.


#. **Disallow insecure keys for authentication.**

   Now that all serivce daemon and client keys have been rotated, you can remove the insecure cipher key type
   from the list of types allowed for authentication.

   .. code:: bash

       ceph mon set auth_allowed_ciphers aes256k

   .. note:: This will now disable the default value for ``mon_auth_allow_insecure_key`` and clear the ``AUTH_INSECURE_KEYS_CREATABLE`` warning.

   .. warning:: If you remove the key type for the ``client.admin`` key or for service daemon keys, you may break authentication in your cluster. That situation will require rescue via :ref:`auth_emergency_allowed_ciphers`. Ensure that ``AUTH_INSECURE_CLIENT_KEY_TYPE`` and ``AUTH_INSECURE_SERVICE_KEY_TYPE`` health warnings are clear!

   Once changed, you should see the :ref:`auth-insecure-keys-allowed` health warning clear.

   .. code:: bash

       ceph --format=json health detail | jq '.checks | has("AUTH_INSECURE_KEYS_ALLOWED") | not'

   output gives ``false``.

At this point, your CephX ciphers and keys should be upgraded.
   

.. _auth_rotate:

Rotating CephX Keys
-------------------

The Monitors provide a mechanism to only update the key for an entity via the
``auth rotate`` command.

.. code:: bash

    ceph auth rotate $TYPE.$ID

For example:

.. code:: bash

    ceph auth rotate client.fs | tee ./client.fs.keyring

The output of the command is the new key:

::

    [client.fs]
            key = <redacted>
            caps mds = "allow rwp"
            caps mon = "allow r"
            caps osd = "allow rw tag cephfs data=*"

This can be imported into a new keyring using ``ceph-authtool``:

.. code:: bash

   ceph-authtool --import-keyring ./client.fs.keyring /etc/ceph/client.fs.keyring


.. note:: The key must be distributed to all locations where the key is in use.


.. _auth_emergency_allowed_ciphers:

Emergency Allowed Ciphers
-------------------------

The Monitors maintain the set of allowed ciphers for credential keys in the ``MonMap``. This is normally set live on the cluster using:

.. prompt: bash

   ceph mon set auth_allowed_ciphers <cipher1,cipher2,...>

It's possible to set a cipher for which no key exists to authenticate during
key upgrades. To work around this, the Monitors may be rescued using the local
startup configuration:

.. confval:: mon_auth_emergency_allowed_ciphers

This will allow your existing ``client.admin`` or other administrative key to authenticate as normal.

When this configuration is set, the Monitors will raise the
:ref:`auth-emergency-ciphers-set` health warning. It should only be set on a
temporary basis to rescue the cluster.


.. _auth_allow_insecure_keys:

Allow Creation of Insecure Keys
-------------------------------

By default, the Monitors will allow creation of keys with a cipher type known
to be insecure so long as the Monitors also allow that cipher to authenticate.
When that cipher type is removed from the authentication list, the Monitors
will also disable the default value of the ``mon_auth_allow_insecure_key``
configuration.

.. confval:: mon_auth_allow_insecure_key

When disabled, ceph commands can no longer create keys with an insecure cipher
type.

When this configuration is enabled by default or otherwise, the Monitors will
raise the :ref:`auth-insecure-keys-creatable` health warning.


.. _auth_dump_keys:

Dump Existing Keys
------------------

The Monitors provide a command to dump all CephX credentials and key metadata
as well as all rotating service key metadata.

.. code:: bash

   ceph --format=json-pretty auth dump-keys

produces truncated output like:

::

    {
        "data": {
            "version": 15,
            "rotating_version": 1,
            "secrets": [
                {
                    "entity": {
                        "type": 2,
                        "type_str": "mds",
                        "id": "a"
                    },
                    "auth": {
                        "key": {
                            "type": 1,
                            "type_str": "aes",
                            "created": "2025-07-29T21:53:30.978646-0400"
                        },
                        "pending_key": {
                            "type": 0,
                            "type_str": "none",
                            "created": "0.000000"
                        },
                        "caps": [
                            {
                                "service_name": "mds",
                                "access_spec": "\u0005\u0000\u0000\u0000allow"
                            },
                            {
                                "service_name": "mgr",
                                "access_spec": "\u0011\u0000\u0000\u0000allow profile mds"
                            },
                            {
                                "service_name": "mon",
                                "access_spec": "\u0011\u0000\u0000\u0000allow profile mds"
                            },
                            {
                                "service_name": "osd",
                                "access_spec": "\u0017\u0000\u0000\u0000allow rw tag cephfs *=*"
                            }
                        ]
                    }
                },
                ...
            ],
            "rotating_secrets": [
                {
                    "entity": {
                        "type": 1,
                        "type_str": "mon",
                        "id": "*"
                    },
                    "secrets": {
                        "max_ver": 3,
                        "keys": [
                            {
                                "id": 1,
                                "expiring_key": {
                                    "key": {
                                        "type": 1,
                                        "type_str": "aes",
                                        "created": "2025-07-29T21:53:04.632712-0400"
                                    },
                                    "expiration": "2025-07-29T22:53:04.632703-0400"
                                }
                            },
                            {
                                "id": 2,
                                "expiring_key": {
                                    "key": {
                                        "type": 1,
                                        "type_str": "aes",
                                        "created": "2025-07-29T21:53:04.632715-0400"
                                    },
                                    "expiration": "2025-07-29T23:53:04.632703-0400"
                                }
                            },
                            {
                                "id": 3,
                                "expiring_key": {
                                    "key": {
                                        "type": 1,
                                        "type_str": "aes",
                                        "created": "2025-07-29T21:53:04.632718-0400"
                                    },
                                    "expiration": "2025-07-30T00:53:04.632703-0400"
                                }
                            }
                        ]
                    }
                },
            ]
        }
    }

This command only works for format types ``json`` or ``json-pretty``.

You may use this information to monitor the entities in the Monitor auth
database as well as key types. For example, this information lets the operator
check if insecure key types are in use. Consider this a low-level API. For
example, the caps listed are in a binary format that is unsuitable for analysis.

.. note:: Generally, the Monitors will warn you if there is a dangerous situation such as insecure key types are in use.



.. _Monitor Bootstrapping: ../../../install/manual-deployment#monitor-bootstrapping
.. _Operating a Cluster: ../../operations/operating
.. _Manual Deployment: ../../../install/manual-deployment
