.. _mgr-smb:

=============================
File System Shares Over SMB
=============================

CephFS access can be provided to clients using the `SMB protocol`_ via the
`Samba suite`_ and `samba-container`_ images - managed by Ceph.

The ``smb`` manager module provides an interface for deploying and controlling
clusters of Samba services as well as managing SMB shares. In the ``smb``
manager module a cluster is a logical management unit that may map to one or
more managed Samba service - by itself a cluster may or may not be using any
high-availability mechanisms.

If the module is not already enabled on your cluster you can enable by running
``ceph mgr module enable smb``.

There are two ways of interacting with the ``smb`` module. The :ref:`imperative
method <mgr-smb-imperative>` uses commands like ``ceph smb cluster create ...``
and ``ceph smb share rm ...`` and should be very familiar to those who have
used Ceph's ``nfs`` manager module on the command line. The :ref:`declarative
method <mgr-smb-declarative>` uses the command ``ceph smb apply`` to process
"resource descriptions" specified in YAML or JSON. This method should be
familiar to those who have used Ceph orchestration with cephadm, just using SMB
specific resource types.

.. note::
   Ceph managed Samba only supports SMB2 and SMB3 versions of the protocol.
   The SMB1 version of the protocol, sometimes known as CIFS, is not supported.
   Some systems, such as the Linux kernel, provide tooling for both SMB1 and SMB2+
   under the CIFS moniker. Check the documentation of the software packages used
   to ensure they support SMB2+ regardless of how the tool is named.

.. note::
   At this time, the ``smb`` module requires cephadm orchestration. It
   does not function without orchestration.

.. _SMB protocol: https://en.wikipedia.org/wiki/Server_Message_Block

.. _Samba suite: https://samba.org

.. _samba-container: https://github.com/samba-in-kubernetes/samba-container

.. _mgr-smb-imperative:

Management Commands - Imperative Style
======================================

Cluster Commands
----------------

Create Cluster
++++++++++++++

.. code:: bash

    $ ceph smb cluster create <cluster_id> {user|active-directory} [--domain-realm=<domain_realm>] [--domain-join-user-pass=<domain_join_user_pass>] [--custom-dns=<custom_dns>] [--placement=<placement>]

Create a new logical cluster, identified by the cluster id value. The cluster
create command must specify the authentication mode the cluster will use. This
may either be one of:

- Custom users and groups, also known as a standalone server, with the ``user``
  keyword
- An Active Directory (AD) domain member server, with the ``active-directory``
  keyword

Options:

cluster_id
    A short string uniquely identifying the cluster
auth_mode
    One of ``user`` or ``active-directory``
domain_realm
    The domain/realm value identifying the AD domain. Required when choosing
    ``active-directory``
domain_join_user_pass
    A string in the form ``<username>%<password>`` that will be used to join
    Samba servers to the AD domain.
custom_dns
    Optional. Can be specified multiple times. One or more IP Addresses that
    will be applied to the Samba containers to override the default DNS
    resolver(s). This option is intended to be used when the host Ceph node is
    not configured to resolve DNS entries within AD domain(s).
placement
    A Ceph orchestration :ref:`placement specifier <orchestrator-cli-placement-spec>`

Remove Cluster
++++++++++++++

.. code:: bash

    $ ceph smb cluster rm <cluster_id>

Remove a logical SMB cluster from the Ceph cluster.

List Clusters
++++++++++++++

.. code:: bash

    $ ceph smb cluster ls [--format=<format>]

Print a listing of cluster ids. The output defaults to JSON, select YAML
encoding with the ``--format=yaml`` option.


Share Commands
--------------

Create Share
++++++++++++

.. code:: bash

    $ ceph smb share create <cluster_id> <share_id> <cephfs_volume> <path> [--share-name=<share_name>] [--subvolume=<subvolume>] [--readonly]

Create a new SMB share, hosted by the named cluster, that maps to the given
CephFS volume and path.

Options:

cluster_id
    A short string uniquely identifying the cluster
share_id
    A short string uniquely identifying the share
cephfs_volume
    The name of the cephfs volume to be shared
path
    A path relative to the root of the volume and/or subvolume
share_name
    Optional. The public name of the share, visible to clients. If not provided
    the ``share_id`` will be used automatically
subvolume
    Optional. A subvolume name in the form ``[<subvolumegroup>/]<subvolume>``.
    The option causes the path to be relative to the CephFS subvolume
    specified.
readonly
    Creates a read-only share

Remove Share
++++++++++++

.. code:: bash

    $ ceph smb share rm <cluster_id> <share_id>

Remove an SMB Share from the cluster.


List Shares
+++++++++++

.. code:: bash

    $ ceph smb share ls <cluster_id> [--format=<format>]

Print a listing of share ids. The output defaults to JSON, select YAML
encoding with the ``--format=yaml`` option.

.. _mgr-smb-declarative:

Management Commands - Declarative Style
=======================================

In addition to the basic imperative management commands the ``smb`` manager
module supports configuration using declarative resource specifications.
Resource specifications can be written in either JSON or YAML. These resource
specifications can be applied to the cluster using the ``ceph smb apply``
command, for example:

.. code:: bash

    $ ceph smb apply -i /path/to/resources.yaml

Resources that have already been applied to the Ceph cluster configuration can
be viewed using the ``ceph smb show`` command. For example:

.. code:: bash

    $ ceph smb show [<resource_name>...]

The ``show`` command can show all resources of a given type or specific
resources by id. ``resource_name`` arguments can take the following forms:

- ``ceph.smb.cluster``: show all cluster resources
- ``ceph.smb.cluster.<cluster_id>``: show specific cluster with given cluster id
- ``ceph.smb.share``: show all share resources
- ``ceph.smb.share.<cluster_id>``: show all share resources part of the given
  cluster
- ``ceph.smb.share.<cluster_id>.<share_id>``: show specific share resource with
  the given cluster and share ids
- ``ceph.smb.usersgroups``: show all Users & Groups resources
- ``ceph.smb.usersgroups.<users_goups_id>``: show a specific Users & Groups
  resource
- ``ceph.smb.join.auth``: show all join auth resources
- ``ceph.smb.join.auth.<auth_id>``: show a specific join auth resource

For example:

.. code:: bash

    $ ceph smb show ceph.smb.cluster.bob ceph.smb.share.bob

Will show one cluster resource (if it exists) for the cluster "bob" as well as
all share resources associated with the cluster "bob".

.. note::
    The `show` subcommand prints out resources in the same form that the
    ``apply`` command accepts, making it possible to "round-trip" values
    between show and apply.


Composing Resource Specifications
---------------------------------

A resource specification is made up of one or more Ceph SMB resource
descriptions written in either JSON or YAML formats. More than one resource
can be specified if the resources are contained within a JSON/YAML *list*,
or a JSON/YAML object containing the key ``resources`` with a corresponding
*list* value containing the resources. Additionally, a YAML specification
may consist of a series of YAML documents each containing a resource.

An example YAML based simple list looks like the following:

.. code-block:: yaml

    - resource_type: ceph.smb.cluster
      cluster_id: rhumba
      # ... other fields skipped for brevity ...
    - resource_type: ceph.smb.cluster
      cluster_id: salsa
      # ... other fields skipped for brevity ...
    - resource_type: ceph.smb.share
      cluster_id: salsa
      share_id: foo
      # ... other fields skipped for brevity ...


An example JSON based simple list looks like the following:

.. code-block:: json

    [
      {"resource_type": "ceph.smb.cluster",
       "cluster_id": "rhumba",
       "...": "... other fields skipped for brevity ..."
      },
      {"resource_type": "ceph.smb.cluster",
       "cluster_id": "salsa",
       "...": "... other fields skipped for brevity ..."
      },
      {"resource_type": "ceph.smb.share",
       "cluster_id": "salsa",
       "share_id": "foo",
       "...": "... other fields skipped for brevity ..."
      }
    ]

An example YAML based resource list looks like the following:

.. code-block:: yaml

    resources:
      - resource_type: ceph.smb.cluster
        cluster_id: rhumba
        # ... other fields skipped for brevity ...
      - resource_type: ceph.smb.cluster
        cluster_id: salsa
        # ... other fields skipped for brevity ...
      - resource_type: ceph.smb.share
        cluster_id: salsa
        share_id: foo
        # ... other fields skipped for brevity ...


An example JSON based resoure list looks like the following:

.. code-block:: json

    {
      "resources": [
        {"resource_type": "ceph.smb.cluster",
         "cluster_id": "rhumba",
         "...": "... other fields skipped for brevity ..."
        },
        {"resource_type": "ceph.smb.cluster",
         "cluster_id": "salsa",
         "...": "... other fields skipped for brevity ..."
        },
        {"resource_type": "ceph.smb.share",
         "cluster_id": "salsa",
         "share_id": "foo",
         "...": "... other fields skipped for brevity ..."
        }
      ]
    }

An example YAML resource list consisting of multiple documents looks like
the following:

.. code-block:: yaml

    ---
    resource_type: ceph.smb.cluster
    cluster_id: rhumba
    # ... other fields skipped for brevity ...
    ---
    resource_type: ceph.smb.cluster
    cluster_id: salsa
    # ... other fields skipped for brevity ...
    ---
    resource_type: ceph.smb.share
    cluster_id: salsa
    share_id: foo
    # ... other fields skipped for brevity ...


Each individual resource description must belong to one of the types described
below.

.. note::
   For brevity, all following examples will use YAML only. Assume that the
   equivalent JSON forms are valid.

Cluster Resource
----------------

A cluster resource supports the following fields:

resource_type
    A literal string ``ceph.smb.cluster``
cluster_id
    A short string identifying the cluster
auth_mode
    One of ``user`` or ``active-directory``
intent
    One of ``present`` or ``removed``. If not provided, ``present`` is
    assumed. If ``removed`` all following fields are optional
domain_settings
    Object. Ignored/optional for ``user`` auth. Required for ``active-directory``
    Fields:

    realm
        Required string. AD domain/realm name.
    join_sources
        Required list. Each element is an object with :ref:`join source fields
        <join-source-fields>`
user_group_settings
    List. Ignored/optional for ``active-directory``. Each element is an object
    with :ref:`user group source fields <user-group-source-fields>`
custom_dns
    Optional. List of IP Addresses. IP addresses will be used as DNS
    resolver(s) in Samba containers allowing the containers to use domain DNS
    even if the Ceph host does not
placement
    Optional. A Ceph Orchestration :ref:`placement specifier
    <orchestrator-cli-placement-spec>`.  Defaults to one host if not provided


.. _join-source-fields:

A join source object supports the following fields:

source_type
    One of ``password`` or ``resource``
auth
    Object. Required for ``source_type: password``. Fields:

    username:
        Required string. User with ability to join a system to AD.
    password:
        Required string. The AD user's password
ref
    String. Required for ``source_type: resource``. Must refer to the ID of a
    ``ceph.smb.join.auth`` resource

.. _user-group-source-fields:

A user group source object supports the following fields:

source_type
    One of ``inline`` or ``resource``
values
    Object. Required for ``source_type: inline``. Fields:

    users
        List of objects. Fields:

        username
            A user name
        password
            A password
    groups
        List of objects. Fields:

        name
            The name of the group
ref
    String. Required for ``source_type: resource``. Must refer to the ID of a
    ``ceph.smb.join.auth`` resource

The following is an example of a cluster configured for AD membership:

.. code-block:: yaml

    resource_type: ceph.smb.cluster
    cluster_id: tango
    auth_mode: active-directory
    domain_settings:
      realm: DOMAIN1.SINK.TEST
      join_sources:
        # this join source refers to a join auth resource with id "join1-admin"
        - source_type: resource
          ref: join1-admin
    custom_dns:
      - "192.168.76.204"
    placement:
      count: 1

The following is an example of a cluster configured for standalone operation:

.. code-block:: yaml

    resource_type: ceph.smb.cluster
    cluster_id: rhumba
    auth_mode: user
    user_group_settings:
      - source_type: inline
        values:
          users:
            - name: chuckx
              password: 3xample101
            - name: steves
              password: F00Bar123
          groups: []
    placement:
      hosts:
        - node6.mycluster.sink.test

An example cluster resource with intent to remove:

.. code-block:: yaml

    resource_type: ceph.smb.cluster
    cluster_id: rhumba
    intent: removed



Share Resource
--------------

A share resource supports the following fields:

resource_type
    A literal string ``ceph.smb.share``
cluster_id
    A short string identifying the cluster
share_id
    A short string identifying the share. Must be Unique within a cluster
intent
    One of ``present`` or ``removed``. If not provided, ``present`` is assumed.
    If ``removed`` all following fields are optional
name
    Optional string. A longer name capable of supporting spaces and other
    characters that will be presented to SMB clients
readonly
    Optional boolean, defaulting to false. If true no clients are permitted to
    write to the share
browseable
    Optional boolean, defaulting to true. If true the share will be included in
    share listings visible to clients
cephfs
    Required object. Fields:

    volume
        Required string. Name of the cephfs volume to use
    path
        Required string. Path within the volume or subvolume to share
    subvolumegroup
        Optional string. Name of a subvolumegroup to share
    subvolume
        Optional string. Name of a subvolume to share. If ``subvolumegroup`` is
        not set and this value contains a exactly one ``/`` character, the
        subvolume field will automatically be split into
        ``<subvolumegroup>/<subvolume>`` parts for convenience
    provider
        Optional. One of ``samba-vfs`` or ``kcephfs`` (``kcephfs`` is not yet
        supported) . Selects how CephFS storage should be provided to the share

The following is an example of a share:

.. code-block:: yaml

    resource_type: ceph.smb.share
    cluster_id: tango
    share_id: sp1
    name: "Staff Pics"
    cephfs:
      volume: cephfs
      path: /pics
      subvolumegroup: smbshares
      subvolume: staff


Another example, this time of a share with an intent to be removed:

.. code-block:: yaml

    resource_type: ceph.smb.share
    cluster_id: tango
    share_id: sp2
    intent: removed


Join-Auth Resource
------------------

A join auth resource supports the following fields:

resource_type
    A literal string ``ceph.smb.join.auth``
auth_id
    A short string identifying the join auth resource
intent
    One of ``present`` or ``removed``. If not provided, ``present`` is assumed.
    If ``removed`` all following fields are optional
auth
    Required object. Fields:

    username
        Required string. User with ability to join a system to AD
    password
        Required string. The AD user's password

Example:

.. code-block:: yaml

    resource_type: ceph.smb.join.auth
    auth_id: join1-admin
    auth:
      username: Administrator
      password: Passw0rd


Users-and-Groups Resource
-------------------------

A users & groups resource supports the following fields:

resource_type
    A literal string ``ceph.smb.usersgroups``
users_groups_id
    A short string identifying the users and groups resource
intent
    One of ``present`` or ``removed``. If not provided, ``present`` is assumed.
    If ``removed`` all following fields are optional.
values
    Required object. Fields:

    users
        List of objects. Fields:

        username
            A user name
        password
            A password
    groups
        List of objects. Fields:

        name
            The name of the group


Example:

.. code-block:: yaml

    resource_type: ceph.smb.usersgroups
    users_groups_id: ug1
    values:
      users:
        - name: chuckx
          password: 3xample101
        - name: steves
          password: F00Bar123
        groups: []


A Declarative Configuration Example
-----------------------------------

Using the resource descriptions above we can put together an example
that creates a cluster and shares from scratch based on a resource
configuration file. First, create the YAML with the contents:

.. code-block:: yaml

    resources:
      # Define an AD member server cluster
      - resource_type: ceph.smb.cluster
        cluster_id: tango
        auth_mode: active-directory
        domain_settings:
          realm: DOMAIN1.SINK.TEST
          join_sources:
            - source_type: resource
              ref: join1-admin
        custom_dns:
          - "192.168.76.204"
        # deploy 1 set of samba containers on a host labeled "ilovesmb"
        placement:
          count: 1
          label: ilovesmb
      # Define a join auth that our cluster will use to join AD
      # Warning: Typically you do not want to use the Administrator user
      # to perform joins on a production AD
      - resource_type: ceph.smb.join.auth
        auth_id: join1-admin
        auth:
          username: Administrator
          password: Passw0rd
      # A share that uses the root of a subvolume
      # The share name is the same as its id
      - resource_type: ceph.smb.share
        cluster_id: tango
        share_id: cache
        cephfs:
          volume: cephfs
          subvolumegroup: smb1
          subvolume: cache
          path: /
      # A share that uses the a sub-dir of a subvolume
      # The share name is not the same as its id
      - resource_type: ceph.smb.share
        cluster_id: tango
        share_id: sp1
        name: "Staff Pics"
        cephfs:
          volume: cephfs
          path: /pics
          subvolumegroup: smb1
          subvolume: staff


Save this text to a YAML file named ``resources.yaml`` and make it available
on a cluster admin host. Then run:

.. code:: bash

    $ ceph smb apply -i resources.yaml

The command will print a summary of the changes made and begin to automatically
deploy the needed resources. See `Accessing Shares`_ for more information
about how to test this example deployment.

Later, if these resources are no longer needed they can be cleaned up in one
action with a new file ``removed.yaml`` containing:

.. code-block:: yaml

    resources:
      - resource_type: ceph.smb.cluster
        cluster_id: tango
        intent: removed
      - resource_type: ceph.smb.join.auth
        auth_id: join1-admin
        intent: removed
      - resource_type: ceph.smb.share
        cluster_id: tango
        share_id: cache
        intent: removed
      - resource_type: ceph.smb.share
        cluster_id: tango
        share_id: sp1
        intent: removed

By issuing the command:

.. code:: bash

    $ ceph smb apply -i removed.yaml


SMB Cluster Management
======================

The ``smb`` module will automatically deploy logical clusters on hosts using
cephadm orchestration. This orchestration is automatically triggered when a
cluster has been configured for at least one share. The ``placement`` field of
the cluster resource is passed onto the orchestration layer and is used to
determine on what nodes of the Ceph cluster Samba containers will be run.

At this time Samba services can only listen on port 445. Due to this
restriction only one Samba server, as part of one cluster, may run on a single
Ceph node at a time. Ensure that the placement specs on each cluster do not
overlap.

The ``smb`` clusters are fully isolated from each other. This means that, as
long as you have sufficient resources in your Ceph cluster, you can run multiple
independent clusters that may or may not join the same AD domains/forests.
However you should not share a directory with multiple different clusters
that may have different authentication modes and/or identity mapping schemes.

.. note::
   Future versions of the ``smb`` module may programatically attempt to prevent
   such conditions.


Accessing Shares
================

Once a cluster and it's component Samba containers have been deployed and the
shares have been configured clients may connect to the servers. Microsoft
Windows systems have SMB support built in and using Windows Explorer a share
can be specified like so: ``\\<hostname>\<sharename>``. For example:
``\\ceph0.mycluster.sink.test\Staff Pics``. The Windows node should
automatically attempt to log into the share. If the cluster and Windows client
are both configured for the same AD Domain then a password-less single sign-on
login will automatically be performed. If the cluster is configured for
``user`` auth, a username and password prompt should appear. Enter one user
name and password combination that was specified in the cluster and/or
``ceph.smb.usersgroups`` resource.

MacOS X systems and many Linux based systems also support connecting to SMB
shares. Consult the documentation for those Operating Systems and Distributions
for how to connect to SMB shares.

A Ceph cluster operator wanting to quickly test a share is functioning may want
to install ``smbclient`` or use the Samba Client Container image available from
the `samba-container`_ project with the image
``quay.io/samba.org/samba-client:latest``. On a client or within the container
run ``smbclient -U <username> //<hostname>/<sharename>`` and enter the password
at the prompt. Refer to the `smbclient documentation`_ for more details.

.. _smbclient documentation:
   https://www.samba.org/samba/docs/current/man-html/smbclient.1.html
