.. _user-management:

=================
 User Management
=================

This document describes :term:`Ceph Client` users, and describes the process by
which they perform authentication and authorization so that they can access the
:term:`Ceph Storage Cluster`. Users are either individuals or system actors
(for example, applications) that use Ceph clients to interact with the Ceph
Storage Cluster daemons.

.. ditaa::
            +-----+
            | {o} |
            |     |
            +--+--+       /---------\               /---------\
               |          |  Ceph   |               |  Ceph   |
            ---+---*----->|         |<------------->|         |
               |     uses | Clients |               | Servers |
               |          \---------/               \---------/
            /--+--\
            |     |
            |     |
             actor


When Ceph runs with authentication and authorization enabled (both are enabled
by default), you must specify a user name and a keyring that contains the
secret key of the specified user (usually these are specified via the command
line). If you do not specify a user name, Ceph will use ``client.admin`` as the
default user name. If you do not specify a keyring, Ceph will look for a
keyring via the ``keyring`` setting in the Ceph configuration. For example, if
you execute the ``ceph health`` command without specifying a user or a keyring,
Ceph will assume that the keyring is in ``/etc/ceph/ceph.client.admin.keyring``
and will attempt to use that keyring. The following illustrates this behavior:

.. prompt:: bash $

   ceph health

Ceph will interpret the command like this:

.. prompt:: bash $

   ceph -n client.admin --keyring=/etc/ceph/ceph.client.admin.keyring health

Alternatively, you may use the ``CEPH_ARGS`` environment variable to avoid
re-entry of the user name and secret.

For details on configuring the Ceph Storage Cluster to use authentication, see
`Cephx Config Reference`_. For details on the architecture of Cephx, see
`Architecture - High Availability Authentication`_.

Background
==========

No matter what type of Ceph client is used (for example: Block Device, Object
Storage, Filesystem, native API), Ceph stores all data as RADOS objects within
`pools`_.  Ceph users must have access to a given pool in order to read and
write data, and Ceph users must have execute permissions in order to use Ceph's
administrative commands. The following concepts will help you understand
Ceph['s] user management.

.. _rados-ops-user:

User
----

A user is either an individual or a system actor (for example, an application).
Creating users allows you to control who (or what) can access your Ceph Storage
Cluster, its pools, and the data within those pools.

Ceph has the concept of a ``type`` of user. For purposes of user management,
the type will always be ``client``. Ceph identifies users in a "period-
delimited form" that consists of the user type and the user ID: for example,
``TYPE.ID``, ``client.admin``, or ``client.user1``. The reason for user typing
is that the Cephx protocol is used not only by clients but also non-clients,
such as Ceph Monitors, OSDs, and Metadata Servers. Distinguishing the user type
helps to distinguish between client users and other users. This distinction
streamlines access control, user monitoring, and traceability.

Sometimes Ceph's user type might seem confusing, because the Ceph command line
allows you to specify a user with or without the type, depending upon your
command line usage. If you specify ``--user`` or ``--id``, you can omit the
type. For example, ``client.user1`` can be entered simply as ``user1``. On the
other hand, if you specify ``--name`` or ``-n``, you must supply the type and
name: for example, ``client.user1``. We recommend using the type and name as a
best practice wherever possible.

.. note:: A Ceph Storage Cluster user is not the same as a Ceph Object Storage
   user or a Ceph File System user. The Ceph Object Gateway uses a Ceph Storage
   Cluster user to communicate between the gateway daemon and the storage
   cluster, but the Ceph Object Gateway has its own user-management
   functionality for end users. The Ceph File System uses POSIX semantics, and
   the user space associated with the Ceph File System is not the same as the
   user space associated with a Ceph Storage Cluster user.

Authorization (Capabilities)
----------------------------

Ceph uses the term "capabilities" (caps) to describe the permissions granted to
an authenticated user to exercise the functionality of the monitors, OSDs, and
metadata servers. Capabilities can also restrict access to data within a pool,
a namespace within a pool, or a set of pools based on their application tags.
A Ceph administrative user specifies the capabilities of a user when creating
or updating that user.

Capability syntax follows this form::

    {daemon-type} '{cap-spec}[, {cap-spec} ...]'

- **Monitor Caps:** Monitor capabilities include ``r``, ``w``, ``x`` access
  settings, and can be applied in aggregate from pre-defined profiles with
  ``profile {name}``. For example::

    mon 'allow {access-spec} [network {network/prefix}]'

    mon 'profile {name}'

  The ``{access-spec}`` syntax is as follows: ::

        * | all | [r][w][x]

  The optional ``{network/prefix}`` is a standard network name and prefix
  length in CIDR notation (for example, ``10.3.0.0/16``).  If
  ``{network/prefix}`` is present, the monitor capability can be used only by
  clients that connect from the specified network.

- **OSD Caps:** OSD capabilities include ``r``, ``w``, ``x``, and
  ``class-read`` and ``class-write`` access settings. OSD capabilities can be
  applied in aggregate from pre-defined profiles with ``profile {name}``. In
  addition, OSD capabilities allow for pool and namespace settings. ::

    osd 'allow {access-spec} [{match-spec}] [network {network/prefix}]'

    osd 'profile {name} [pool={pool-name} [namespace={namespace-name}]] [network {network/prefix}]'

  There are two alternative forms of the ``{access-spec}`` syntax: ::

        * | all | [r][w][x] [class-read] [class-write]

        class {class name} [{method name}]

  There are two alternative forms of the optional ``{match-spec}`` syntax::

        pool={pool-name} [namespace={namespace-name}] [object_prefix {prefix}]

        [namespace={namespace-name}] tag {application} {key}={value}

  The optional ``{network/prefix}`` is a standard network name and prefix
  length in CIDR notation (for example, ``10.3.0.0/16``). If
  ``{network/prefix}`` is present, the OSD capability can be used only by
  clients that connect from the specified network.

- **Manager Caps:** Manager (``ceph-mgr``) capabilities include ``r``, ``w``,
  ``x`` access settings, and can be applied in aggregate from pre-defined
  profiles with ``profile {name}``. For example::

    mgr 'allow {access-spec} [network {network/prefix}]'

    mgr 'profile {name} [{key1} {match-type} {value1} ...] [network {network/prefix}]'

  Manager capabilities can also be specified for specific commands, for all
  commands exported by a built-in manager service, or for all commands exported
  by a specific add-on module. For example::

        mgr 'allow command "{command-prefix}" [with {key1} {match-type} {value1} ...] [network {network/prefix}]'

        mgr 'allow service {service-name} {access-spec} [network {network/prefix}]'

        mgr 'allow module {module-name} [with {key1} {match-type} {value1} ...] {access-spec} [network {network/prefix}]'

  The ``{access-spec}`` syntax is as follows: ::

        * | all | [r][w][x]

  The ``{service-name}`` is one of the following: ::

        mgr | osd | pg | py

  The ``{match-type}`` is one of the following: ::

        = | prefix | regex

- **Metadata Server Caps:** For administrators, use ``allow *``. For all other
  users (for example, CephFS clients), consult :doc:`/cephfs/client-auth`

.. note:: The Ceph Object Gateway daemon (``radosgw``) is a client of the
          Ceph Storage Cluster. For this reason, it is not represented as 
          a Ceph Storage Cluster daemon type.

The following entries describe access capabilities.

``allow``

:Description: Precedes access settings for a daemon. Implies ``rw``
              for MDS only.


``r``

:Description: Gives the user read access. Required with monitors to retrieve
              the CRUSH map.


``w``

:Description: Gives the user write access to objects.


``x``

:Description: Gives the user the capability to call class methods
              (that is, both read and write) and to conduct ``auth``
              operations on monitors.


``class-read``

:Descriptions: Gives the user the capability to call class read methods.
               Subset of ``x``.


``class-write``

:Description: Gives the user the capability to call class write methods.
              Subset of ``x``.


``*``, ``all``

:Description: Gives the user read, write, and execute permissions for a
              particular daemon/pool, as well as the ability to execute
              admin commands.


The following entries describe valid capability profiles:

``profile osd`` (Monitor only)

:Description: Gives a user permissions to connect as an OSD to other OSDs or
              monitors. Conferred on OSDs in order to enable OSDs to handle replication
              heartbeat traffic and status reporting.


``profile mds`` (Monitor only)

:Description: Gives a user permissions to connect as an MDS to other MDSs or
              monitors.


``profile bootstrap-osd`` (Monitor only)

:Description: Gives a user permissions to bootstrap an OSD. Conferred on
              deployment tools such as ``ceph-volume`` and ``cephadm``
              so that they have permissions to add keys when
              bootstrapping an OSD.


``profile bootstrap-mds`` (Monitor only)

:Description: Gives a user permissions to bootstrap a metadata server.
              Conferred on deployment tools such as ``cephadm``
              so that they have permissions to add keys when bootstrapping
              a metadata server.

``profile bootstrap-rbd`` (Monitor only)

:Description: Gives a user permissions to bootstrap an RBD user.
              Conferred on deployment tools such as ``cephadm``
              so that they have permissions to add keys when bootstrapping
              an RBD user.

``profile bootstrap-rbd-mirror`` (Monitor only)

:Description: Gives a user permissions to bootstrap an ``rbd-mirror`` daemon
              user. Conferred on deployment tools such as ``cephadm`` so that
              they have permissions to add keys when bootstrapping an
              ``rbd-mirror`` daemon.

``profile rbd`` (Manager, Monitor, and OSD)

:Description: Gives a user permissions to manipulate RBD images. When used as a
              Monitor cap, it provides the user with the minimal privileges
              required by an RBD client application; such privileges include
              the ability to blocklist other client users. When used as an OSD
              cap, it provides an RBD client application with read-write access
              to the specified pool. The Manager cap supports optional ``pool``
              and ``namespace`` keyword arguments.

``profile rbd-mirror`` (Monitor only)

:Description: Gives a user permissions to manipulate RBD images and retrieve
              RBD mirroring config-key secrets. It provides the minimal
              privileges required for the user to manipulate the ``rbd-mirror``
              daemon.

``profile rbd-read-only`` (Manager and OSD)

:Description: Gives a user read-only permissions to RBD images. The Manager cap
              supports optional ``pool`` and ``namespace`` keyword arguments.

``profile simple-rados-client`` (Monitor only)

:Description: Gives a user read-only permissions for monitor, OSD, and PG data.
              Intended for use by direct librados client applications.

``profile simple-rados-client-with-blocklist`` (Monitor only)

:Description: Gives a user read-only permissions for monitor, OSD, and PG data.
              Intended for use by direct librados client applications. Also
              includes permissions to add blocklist entries to build
              high-availability (HA) applications.

``profile fs-client`` (Monitor only)

:Description: Gives a user read-only permissions for monitor, OSD, PG, and MDS
              data. Intended for CephFS clients.

``profile role-definer`` (Monitor and Auth)

:Description: Gives a user **all** permissions for the auth subsystem, read-only
              access to monitors, and nothing else. Useful for automation
              tools. Do not assign this unless you really, **really** know what
              you're doing, as the security ramifications are substantial and
              pervasive.

``profile crash`` (Monitor and MGR)

:Description: Gives a user read-only access to monitors. Used in conjunction
              with the manager ``crash`` module to upload daemon crash
              dumps into monitor storage for later analysis.

Pool
----

A pool is a logical partition where users store data.
In Ceph deployments, it is common to create a pool as a logical partition for
similar types of data. For example, when deploying Ceph as a back end for
OpenStack, a typical deployment would have pools for volumes, images, backups
and virtual machines, and such users as ``client.glance`` and ``client.cinder``.

Application Tags
----------------

Access may be restricted to specific pools as defined by their application
metadata. The ``*`` wildcard may be used for the ``key`` argument, the
``value`` argument, or both. The ``all`` tag is a synonym for ``*``.

Namespace
---------

Objects within a pool can be associated to a namespace: that is, to a logical group of
objects within the pool. A user's access to a pool can be associated with a
namespace so that reads and writes by the user can take place only within the
namespace. Objects written to a namespace within the pool can be accessed only
by users who have access to the namespace.

.. note:: Namespaces are primarily useful for applications written on top of
   ``librados``. In such situations, the logical grouping provided by
   namespaces  can obviate the need to create different pools. In Luminous and
   later releases, Ceph Object Gateway uses namespaces for various metadata
   objects.

The rationale for namespaces is this: namespaces are relatively less
computationally expensive than pools, which (pools) can be a computationally
expensive method of segregating data sets between different authorized users.

For example, a pool ought to host approximately 100 placement-group replicas
per OSD. This means that a cluster with 1000 OSDs and three 3R replicated pools
would have (in a single pool) 100,000 placement-group replicas, and that means
that it has 33,333 Placement Groups.

By contrast, writing an object to a namespace simply associates the namespace
to the object name without incurring the computational overhead of a separate
pool. Instead of creating a separate pool for a user or set of users, you can
use a namespace. 

.. note::

   Namespaces are available only when using ``librados``.


Access may be restricted to specific RADOS namespaces by use of the ``namespace``
capability. Limited globbing of namespaces (that is, use of wildcards (``*``)) is supported: if the last character
of the specified namespace is ``*``, then access is granted to any namespace
starting with the provided argument.

Managing Users
==============

User management functionality provides Ceph Storage Cluster administrators with
the ability to create, update, and delete users directly in the Ceph Storage
Cluster.

When you create or delete users in the Ceph Storage Cluster, you might need to
distribute keys to clients so that they can be added to keyrings. For details, see `Keyring
Management`_.

Listing Users
-------------

To list the users in your cluster, run the following command:

.. prompt:: bash $

    ceph auth ls

Ceph will list all users in your cluster. For example, in a two-node
cluster, ``ceph auth ls`` will provide an output that resembles the following::

    installed auth entries:

    osd.0
        key: AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==
        caps: [mon] allow profile osd
        caps: [osd] allow *
    osd.1
        key: AQC4CbtTCFJBChAAVq5spj0ff4eHZICxIOVZeA==
        caps: [mon] allow profile osd
        caps: [osd] allow *
    client.admin
        key: AQBHCbtT6APDHhAA5W00cBchwkQjh3dkKsyPjw==
        caps: [mds] allow
        caps: [mon] allow *
        caps: [osd] allow *
    client.bootstrap-mds
        key: AQBICbtTOK9uGBAAdbe5zcIGHZL3T/u2g6EBww==
        caps: [mon] allow profile bootstrap-mds
    client.bootstrap-osd
        key: AQBHCbtT4GxqORAADE5u7RkpCN/oo4e5W0uBtw==
        caps: [mon] allow profile bootstrap-osd

Note that, according to the ``TYPE.ID`` notation for users, ``osd.0`` is a
user of type ``osd`` and an ID of ``0``, and ``client.admin`` is a user of type
``client`` and an ID of ``admin`` (that is, the default ``client.admin`` user).
Note too that each entry has a ``key: <value>`` entry, and also has one or more
``caps:`` entries.

To save the output of ``ceph auth ls`` to a file, use the ``-o {filename}`` option.


Getting a User
--------------

To retrieve a specific user, key, and capabilities, run the following command:

.. prompt:: bash $

   ceph auth get {TYPE.ID}

For example:

.. prompt:: bash $

   ceph auth get client.admin

To save the output of ``ceph auth get`` to a file, use the ``-o {filename}`` option. Developers may also run the following command:

.. prompt:: bash $

   ceph auth export {TYPE.ID}

The ``auth export`` command is identical to ``auth get``.

.. _rados_ops_adding_a_user:

Adding a User
-------------

Adding a user creates a user name (that is, ``TYPE.ID``), a secret key, and
any capabilities specified in the command that creates the user.

A user's key allows the user to authenticate with the Ceph Storage Cluster.
The user's capabilities authorize the user to read, write, or execute on Ceph
monitors (``mon``), Ceph OSDs (``osd``) or Ceph Metadata Servers (``mds``).

There are a few ways to add a user:

- ``ceph auth add``: This command is the canonical way to add a user. It
  will create the user, generate a key, and add any specified capabilities.

- ``ceph auth get-or-create``: This command is often the most convenient way
  to create a user, because it returns a keyfile format with the user name
  (in brackets) and the key. If the user already exists, this command
  simply returns the user name and key in the keyfile format. To save the output to
  a file, use the ``-o {filename}`` option.

- ``ceph auth get-or-create-key``: This command is a convenient way to create
  a user and return the user's key and nothing else. This is useful for clients that
  need only the key (for example, libvirt). If the user already exists, this command
  simply returns the key. To save the output to
  a file, use the ``-o {filename}`` option.

It is possible, when creating client users, to create a user with no capabilities. A user
with no capabilities is useless beyond mere authentication, because the client
cannot retrieve the cluster map from the monitor. However, you might want to create a user
with no capabilities and wait until later to add capabilities to the user by using the ``ceph auth caps`` comand.

A typical user has at least read capabilities on the Ceph monitor and
read and write capabilities on Ceph OSDs. A user's OSD permissions
are often restricted so that the user can access only one particular pool.
In the following example, the commands (1) add a client named ``john`` that has read capabilities on the Ceph monitor
and read and write capabilities on the pool named ``liverpool``, (2) authorize a client named ``paul`` to have read capabilities on the Ceph monitor and
read and write capabilities on the pool named ``liverpool``, (3) authorize a client named ``george`` to have read capabilities on the Ceph monitor and
read and write capabilities on the pool named ``liverpool`` and use the keyring named ``george.keyring`` to make this authorization, and (4) authorize
a client named ``ringo`` to have read capabilities on the Ceph monitor and read and write capabilities on the pool named ``liverpool`` and use the key
named ``ringo.key`` to make this authorization:

.. prompt:: bash $

   ceph auth add client.john mon 'allow r' osd 'allow rw pool=liverpool'
   ceph auth get-or-create client.paul mon 'allow r' osd 'allow rw pool=liverpool'
   ceph auth get-or-create client.george mon 'allow r' osd 'allow rw pool=liverpool' -o george.keyring
   ceph auth get-or-create-key client.ringo mon 'allow r' osd 'allow rw pool=liverpool' -o ringo.key

.. important:: Any user that has capabilities on OSDs will have access to ALL pools in the cluster
   unless that user's access has been restricted to a proper subset of the pools in the cluster.


.. _modify-user-capabilities:

Modifying User Capabilities
---------------------------

The ``ceph auth caps`` command allows you to specify a user and change that
user's capabilities. Setting new capabilities will overwrite current capabilities.
To view current capabilities, run ``ceph auth get USERTYPE.USERID``. 
To add capabilities, run a command of the following form (and be sure to specify the existing capabilities):

.. prompt:: bash $

   ceph auth caps USERTYPE.USERID {daemon} 'allow [r|w|x|*|...] [pool={pool-name}] [namespace={namespace-name}]' [{daemon} 'allow [r|w|x|*|...] [pool={pool-name}] [namespace={namespace-name}]']

For example:

.. prompt:: bash $

   ceph auth get client.john
   ceph auth caps client.john mon 'allow r' osd 'allow rw pool=liverpool'
   ceph auth caps client.paul mon 'allow rw' osd 'allow rwx pool=liverpool'
   ceph auth caps client.brian-manager mon 'allow *' osd 'allow *'

For additional details on capabilities, see `Authorization (Capabilities)`_.

Deleting a User
---------------

To delete a user, use ``ceph auth del``:

.. prompt:: bash $

   ceph auth del {TYPE}.{ID}

Here ``{TYPE}`` is either ``client``, ``osd``, ``mon``, or ``mds``,
and ``{ID}`` is the user name or the ID of the daemon.


Printing a User's Key
---------------------

To print a user's authentication key to standard output, run the following command:

.. prompt:: bash $

   ceph auth print-key {TYPE}.{ID}

Here ``{TYPE}`` is either ``client``, ``osd``, ``mon``, or ``mds``,
and ``{ID}`` is the user name or the ID of the daemon.

When it is necessary to populate client software with a user's key (as in the case of libvirt),
you can print the user's key by running the following command:

.. prompt:: bash $

   mount -t ceph serverhost:/ mountpoint -o name=client.user,secret=`ceph auth print-key client.user`

Importing a User
----------------

To import one or more users, use ``ceph auth import`` and
specify a keyring as follows:

.. prompt:: bash $

   ceph auth import -i /path/to/keyring

For example:

.. prompt:: bash $

   sudo ceph auth import -i /etc/ceph/ceph.keyring

.. note:: The Ceph storage cluster will add new users, their keys, and their
   capabilities and will update existing users, their keys, and their
   capabilities.

Keyring Management
==================

When you access Ceph via a Ceph client, the Ceph client will look for a local
keyring. Ceph presets the ``keyring`` setting with four keyring
names by default. For this reason, you do not have to set the keyring names in your Ceph configuration file
unless you want to override these defaults (which is not recommended). The four default keyring names are as follows:

- ``/etc/ceph/$cluster.$name.keyring``
- ``/etc/ceph/$cluster.keyring``
- ``/etc/ceph/keyring``
- ``/etc/ceph/keyring.bin``

The ``$cluster`` metavariable found in the first two default keyring names above
is your Ceph cluster name as defined by the name of the Ceph configuration
file: for example, if the Ceph configuration file is named ``ceph.conf``,
then your Ceph cluster name is ``ceph`` and the second name above would be
``ceph.keyring``. The ``$name`` metavariable is the user type and user ID:
for example, given the user ``client.admin``, the first name above would be
``ceph.client.admin.keyring``.

.. note:: When running commands that read or write to ``/etc/ceph``, you might
   need to use ``sudo`` to run the command as ``root``.

After you create a user (for example, ``client.ringo``), you must get the key and add
it to a keyring on a Ceph client so that the user can access the Ceph Storage
Cluster.

The `User Management`_ section details how to list, get, add, modify, and delete
users directly in the Ceph Storage Cluster. In addition, Ceph provides the
``ceph-authtool`` utility to allow you to manage keyrings from a Ceph client.

Creating a Keyring
------------------

When you use the procedures in the `Managing Users`_ section to create users,
you must provide user keys to the Ceph client(s). This is required so that the Ceph client(s)
can retrieve the key for the specified user and authenticate that user against the Ceph
Storage Cluster. Ceph clients access keyrings in order to look up a user name and
retrieve the user's key.

The ``ceph-authtool`` utility allows you to create a keyring. To create an
empty keyring, use ``--create-keyring`` or ``-C``. For example:

.. prompt:: bash $

   ceph-authtool --create-keyring /path/to/keyring

When creating a keyring with multiple users, we recommend using the cluster name
(of the form ``$cluster.keyring``) for the keyring filename and saving the keyring in the
``/etc/ceph`` directory. By doing this, you ensure that the ``keyring`` configuration default setting
will pick up the filename without requiring you to specify the filename in the local copy
of your Ceph configuration file. For example, you can create ``ceph.keyring`` by
running the following command:

.. prompt:: bash $

   sudo ceph-authtool -C /etc/ceph/ceph.keyring

When creating a keyring with a single user, we recommend using the cluster name,
the user type, and the user name, and saving the keyring in the ``/etc/ceph`` directory.
For example, we recommend that the ``client.admin`` user use ``ceph.client.admin.keyring``.

To create a keyring in ``/etc/ceph``, you must do so as ``root``. This means
that the file will have ``rw`` permissions for the ``root`` user only, which is
appropriate when the keyring contains administrator keys. However, if you
intend to use the keyring for a particular user or group of users, be sure to use ``chown`` or ``chmod`` to establish appropriate keyring
ownership and access.

Adding a User to a Keyring
--------------------------

When you :ref:`Add a user<rados_ops_adding_a_user>` to the Ceph Storage
Cluster, you can use the `Getting a User`_ procedure to retrieve a user, key,
and capabilities and then save the user to a keyring.

If you want to use only one user per keyring, the `Getting a User`_ procedure with
the ``-o`` option will save the output in the keyring file format. For example,
to create a keyring for the ``client.admin`` user, run the following command:

.. prompt:: bash $

   sudo ceph auth get client.admin -o /etc/ceph/ceph.client.admin.keyring

Notice that the file format in this command is the file format conventionally used when manipulating the keyrings of individual users.

If you want to import users to a keyring, you can use ``ceph-authtool``
to specify the destination keyring and the source keyring.
For example:

.. prompt:: bash $

   sudo ceph-authtool /etc/ceph/ceph.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring

Creating a User
---------------

Ceph provides the `Adding a User`_ function to create a user directly in the Ceph
Storage Cluster. However, you can also create a user, keys, and capabilities
directly on a Ceph client keyring, and then import the user to the Ceph
Storage Cluster. For example:

.. prompt:: bash $

   sudo ceph-authtool -n client.ringo --cap osd 'allow rwx' --cap mon 'allow rwx' /etc/ceph/ceph.keyring

For additional details on capabilities, see `Authorization (Capabilities)`_.

You can also create a keyring and add a new user to the keyring simultaneously.
For example:

.. prompt:: bash $

   sudo ceph-authtool -C /etc/ceph/ceph.keyring -n client.ringo --cap osd 'allow rwx' --cap mon 'allow rwx' --gen-key

In the above examples, the new user ``client.ringo`` has been added only to the
keyring. The new user has not been added to the Ceph Storage Cluster.

To add the new user ``client.ringo`` to the Ceph Storage Cluster, run the following command:

.. prompt:: bash $

   sudo ceph auth add client.ringo -i /etc/ceph/ceph.keyring

Modifying a User
----------------

To modify the capabilities of a user record in a keyring, specify the keyring
and the user, followed by the capabilities. For example:

.. prompt:: bash $

   sudo ceph-authtool /etc/ceph/ceph.keyring -n client.ringo --cap osd 'allow rwx' --cap mon 'allow rwx'

To update the user in the Ceph Storage Cluster, you must update the user
in the keyring to the user entry in the Ceph Storage Cluster. To do so, run the following command:

.. prompt:: bash $

   sudo ceph auth import -i /etc/ceph/ceph.keyring

For details on updating a Ceph Storage Cluster user from a
keyring, see `Importing a User`_

You may also :ref:`Modify user capabilities<modify-user-capabilities>` directly in the cluster, store the
results to a keyring file, and then import the keyring into your main
``ceph.keyring`` file.

Command Line Usage
==================

Ceph supports the following usage for user name and secret:

``--id`` | ``--user``

:Description: Ceph identifies users with a type and an ID: the form of this user identification is ``TYPE.ID``, and examples of the type and ID are
              ``client.admin`` and ``client.user1``. The ``id``, ``name`` and
              ``-n`` options allow you to specify the ID portion of the user
              name (for example, ``admin``, ``user1``, ``foo``). You can specify
              the user with the ``--id`` and omit the type. For example,
              to specify user ``client.foo``, run the following commands:

              .. prompt:: bash $

                 ceph --id foo --keyring /path/to/keyring health
                 ceph --user foo --keyring /path/to/keyring health


``--name`` | ``-n``

:Description: Ceph identifies users with a type and an ID: the form of this user identification is ``TYPE.ID``, and examples of the type and ID are
              ``client.admin`` and ``client.user1``. The ``--name`` and ``-n``
              options allow you to specify the fully qualified user name.
              You are required to specify the user type (typically ``client``) with the
              user ID. For example:

              .. prompt:: bash $

                 ceph --name client.foo --keyring /path/to/keyring health
                 ceph -n client.foo --keyring /path/to/keyring health


``--keyring``

:Description: The path to the keyring that contains one or more user names and
              secrets. The ``--secret`` option provides the same functionality,
              but it does not work with Ceph RADOS Gateway, which uses
              ``--secret`` for another purpose. You may retrieve a keyring with
              ``ceph auth get-or-create`` and store it locally. This is a
              preferred approach, because you can switch user names without
              switching the keyring path. For example:

              .. prompt:: bash $

                 sudo rbd map --id foo --keyring /path/to/keyring mypool/myimage


.. _pools: ../pools

Limitations
===========

The ``cephx`` protocol authenticates Ceph clients and servers to each other. It
is not intended to handle authentication of human users or application programs
that are run on their behalf. If your access control
needs require that kind of authentication, you will need to have some other mechanism, which is likely to be specific to the
front end that is used to access the Ceph object store. This other mechanism would ensure that only acceptable users and programs are able to run on the
machine that Ceph permits to access its object store.

The keys used to authenticate Ceph clients and servers are typically stored in
a plain text file on a trusted host. Appropriate permissions must be set on the plain text file.

.. important:: Storing keys in plaintext files has security shortcomings, but
   they are difficult to avoid, given the basic authentication methods Ceph
   uses in the background. Anyone setting up Ceph systems should be aware of
   these shortcomings.

In particular, user machines, especially portable machines, should not
be configured to interact directly with Ceph, since that mode of use would
require the storage of a plaintext authentication key on an insecure machine.
Anyone who stole that machine or obtained access to it could
obtain a key that allows them to authenticate their own machines to Ceph.

Instead of permitting potentially insecure machines to access a Ceph object
store directly, you should require users to sign in to a trusted machine in
your environment, using a method that provides sufficient security for your
purposes. That trusted machine will store the plaintext Ceph keys for the
human users. A future version of Ceph might address these particular
authentication issues more fully.

At present, none of the Ceph authentication protocols provide secrecy for
messages in transit. As a result, an eavesdropper on the wire can hear and understand
all data sent between clients and servers in Ceph, even if the eavesdropper cannot create or
alter the data. Similarly, Ceph does not include options to encrypt user data in the
object store. Users can, of course, hand-encrypt and store their own data in the Ceph
object store, but Ceph itself provides no features to perform object
encryption. Anyone storing sensitive data in Ceph should consider
encrypting their data before providing it to the Ceph system.


.. _Architecture - High Availability Authentication: ../../../architecture#high-availability-authentication
.. _Cephx Config Reference: ../../configuration/auth-config-ref
