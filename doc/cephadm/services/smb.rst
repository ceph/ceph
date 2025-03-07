.. _deploy-cephadm-smb-samba:

===========
SMB Service
===========

.. warning::

    SMB support is under active development and many features may be
    missing or immature. A Ceph MGR module, named smb, is available to help
    organize and manage SMB related featues. Unless the smb module
    has been determined to be unsuitable for your needs we recommend using that
    module over directly using the smb service spec.


Deploying Samba Containers
==========================

Cephadm deploys `Samba <http://www.samba.org>`_ servers using container images
built by the `samba-container project <http://github.com/samba-in-kubernetes/samba-container>`_.

In order to host SMB Shares with access to CephFS file systems, deploy
Samba Containers with the following command:

.. prompt:: bash #

    ceph orch apply smb <cluster_id> <config_uri> [--features ...] [--placement ...] ...

There are a number of additional parameters that the command accepts. See
the Service Specification for a description of these options.

Service Specification
=====================

An SMB Service can be applied using a specification. An example in YAML follows:

.. code-block:: yaml

    service_type: smb
    service_id: tango
    placement:
      hosts:
        - ceph0
    spec:
      cluster_id: tango
      features:
        - domain
      config_uri: rados://.smb/tango/scc.toml
      custom_dns:
        - "192.168.76.204"
      join_sources:
        - "rados:mon-config-key:smb/config/tango/join1.json"
      include_ceph_users:
        - client.smb.fs.cluster.tango

The specification can then be applied by running the following command:

.. prompt:: bash #

   ceph orch apply -i smb.yaml


Service Spec Options
--------------------

Fields specific to the ``spec`` section of the SMB Service are described below.

cluster_id
    A short name identifying the SMB "cluster". In this case a cluster is
    simply a management unit of one or more Samba services sharing a common
    configuration, and may not provide actual clustering or availability
    mechanisms.

features
    A list of pre-defined terms enabling specific deployment characteristics.
    An empty list is valid. Supported terms:

    * ``domain``: Enable domain member mode
    * ``clustered``: Enable Samba native cluster mode

config_uri
    A string containing a (standard or de-facto) URI that identifies a
    configuration source that should be loaded by the samba-container as the
    primary configuration file.
    Supported URI schemes include ``http:``, ``https:``, ``rados:``, and
    ``rados:mon-config-key:``.

user_sources
    A list of strings with (standard or de-facto) URI values that will
    be used to identify where credentials for authentication are located.
    See ``config_uri`` for the supported list of URI schemes.

join_sources
    A list of strings with (standard or de-facto) URI values that will
    be used to identify where authentication data that will be used to
    perform domain joins are located. Each join source is tried in sequence
    until one succeeds.
    See ``config_uri`` for the supported list of URI schemes.

custom_dns
    A list of IP addresses that will be used as the DNS servers for a Samba
    container. This features allows Samba Containers to integrate with
    Active Directory even if the Ceph host nodes are not tied into the Active
    Directory DNS domain(s).

include_ceph_users
    A list of cephx user (aka entity) names that the Samba Containers may use.
    The cephx keys for each user in the list will automatically be added to
    the keyring in the container.

cluster_meta_uri
    A string containing a URI that identifies where the cluster structure
    metadata will be stored. Required if ``clustered`` feature is set. Must be
    a RADOS pseudo-URI.

cluster_lock_uri
    A string containing a URI that identifies where Samba/CTDB will store a
    cluster lock. Required if ``clustered`` feature is set. Must be a RADOS
    pseudo-URI.

cluster_public_addrs
    List of objects; optional. Supported only when using Samba's clustering.
    Assign "virtual" IP addresses that will be managed by the clustering
    subsystem and may automatically move between nodes running Samba
    containers.
    Fields:

    address
        Required string. An IP address with a required prefix length (example:
        ``192.168.4.51/24``). This address will be assigned to one of the
        host's network devices and managed automatically.
    destination
        Optional. String or list of strings. A ``destination`` defines where
        the system will assign the managed IPs. Each string value must be a
        network address (example ``192.168.4.0/24``). One or more destinations
        may be supplied. The typical case is to use exactly one destination and
        so the value may be supplied as a string, rather than a list with a
        single item. Each destination network will be mapped to a device on a
        host. Run ``cephadm list-networks`` for an example of these mappings.
        If destination is not supplied the network is automatically determined
        using the address value supplied and taken as the destination.


.. note::

   If one desires clustering between smbd instances (also known as
   High-Availability or "transparent state migration") the feature flag
   ``clustered`` is needed. If this flag is not specified cephadm may deploy
   multiple smb servers but they will lack the coordination needed of an actual
   Highly-Avaiable cluster. When the ``clustered`` flag is specified cephadm
   will deploy additional containers that manage this coordination.
   Additionally, the cluster_meta_uri and cluster_lock_uri values must be
   specified. The former is used by cephadm to describe the smb cluster layout
   to the samba containers. The latter is used by Samba's CTDB component to
   manage an internal cluster lock.


Configuring an SMB Service
--------------------------

.. warning::

   A Manager module for SMB is under active development. Once that module
   is available it will be the preferred method for managing Samba on Ceph
   in an end-to-end manner. The following discussion is provided for the sake
   of completeness and to explain how the software layers interact.

Creating an SMB Service spec is not sufficient for complete operation of a
Samba Container on Ceph. It is important to create valid configurations and
place them in locations that the container can read. The complete specification
of these configurations is out of scope for this document. You can refer to the
`documentation for Samba <https://wiki.samba.org/index.php/Main_Page>`_ as
well as the `samba server container
<https://github.com/samba-in-kubernetes/samba-container/blob/master/docs/server.md>`_
and the `configuation file
<https://github.com/samba-in-kubernetes/sambacc/blob/master/docs/configuration.md>`_
it accepts.

When one has composed a configuration it should be stored in a location
that the Samba Container can access. The recommended approach for running
Samba Containers within Ceph orchestration is to store the configuration
in the Ceph cluster. There are a few ways to store the configuration
in ceph:

RADOS
~~~~~

A configuration file can be stored as a RADOS object in a pool
named ``.smb``. Within the pool there should be a namespace named after the
``cluster_id`` value. The URI used to identify this resource should be
constructed like ``rados://.smb/<cluster_id>/<object_name>``. Example:
``rados://.smb/tango/config.json``.

The containers are automatically deployed with cephx keys allowing access to
resources in these pools and namespaces. As long as this scheme is used
no additional configuration to read the object is needed.

To copy a configuration file to a RADOS pool, use the ``rados`` command line
tool. For example:

.. prompt:: bash #

    # assuming your config file is /tmp/config.json
    rados --pool=.smb --namespace=tango put config.json /tmp/config.json

MON Key/Value Store
~~~~~~~~~~~~~~~~~~~

A configuration file can be stored as a value in the Ceph Monitor Key/Value
store.  The key must be named after the cluster like so:
``smb/config/<cluster_id>/<name>``.  This results in a URI that can be used to
identify this configuration constructed like
``rados:mon-config-key:smb/config/<cluster_id>/<name>``.
Example: ``rados:mon-config-key:smb/config/tango/config.json``.

The containers are automatically deployed with cephx keys allowing access to
resources with the key-prefix ``smb/config/<cluster_id>/``. As long as this
scheme is used no additional configuration to read the value is needed.

To copy a configuration file into the Key/Value store use the ``ceph config-key
put ...`` tool. For example:

.. prompt:: bash #

    # assuming your config file is /tmp/config.json
    ceph config-key set smb/config/tango/config.json -i /tmp/config.json


HTTP/HTTPS
~~~~~~~~~~

A configuration file can be stored on an HTTP(S) server and automatically read
by the Samba Container. Managing a configuration file on HTTP(S) is left as an
exercise for the reader.

.. note:: All URI schemes are supported by parameters that accept URIs. Each
   scheme has different performance and security characteristics.


Limitations
===========

A non-exhaustive list of important limitations for the SMB service follows:

* DNS is a critical component of Active Directory. If one is configuring the
  SMB service for domain membership, either the Ceph host node must be
  configured so that it can resolve the Active Directory (AD) domain or the
  ``custom_dns`` option may be used. In both cases DNS hosts for the AD domain
  must still be reachable from whatever network segment the ceph cluster is on.
* Services must bind to TCP port 445. Running multiple SMB services on the same
  node is not yet supported and will trigger a port-in-use conflict.
