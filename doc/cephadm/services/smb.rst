.. _deploy-cephadm-smb-samba:

===========
SMB Service
===========

.. note:: Only the SMB3 protocol is supported.

.. warning::

    SMB support is under active development and many features may be
    missing or immature. Additionally, a Manager module to automate
    smb clusters and smb shares is in development. Once that feature
    is developed it will be the preferred method for managing
    smb on ceph.


Deploying Samba Containers
==========================

Cephadm deploys `Samba <http://www.samba.org>`_ servers using container images
built by the `samba-container project <http://github.com/samba-in-kubernetes/samba-container>`_.

In order to host SMB Shares with access to CephFS file systems, deploy
Samba Containers with the following command:

.. prompt:: bash #

    orch apply smb <cluster_id> <config_uri> [--features ...] [--placement ...] ...

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
    A short name identifying the smb "cluster". In this case a cluster is
    simply a management unit of one or more Samba services sharing a common
    configuration, and may not provide actual clustering or availability
    mechanisms.

features
    A list of pre-defined terms enabling specific deployment characteristics.
    An empty list is valid. Supported terms:

    * ``domain``: Enable domain member mode

config_uri
    A string containing a (standard or de-facto) URI that identifies a
    configuration source that should be loaded by the samba-container as the
    primary configuration file.
    Supported URI schemes include ``http:``, ``https:``, ``rados:``, and
    ``rados:mon-config-key:``.

join_sources
    A list of strings with (standard or de-facto) URI values that will
    be used to identify where authentication data that will be used to
    perform domain joins are located. Each join source is tried in sequence
    until one succeeds.
    See ``config_uri`` for the supported list of URI schemes.

custom_dns
    A list of IP addresses that will be used as the dns servers for a Samba
    container. This features allows the Samba Containers to integrate with
    Active Directory even if the ceph host nodes are not tied into the Active
    Directory DNS domain(s).

include_ceph_users:
    A list of cephx user (aka entity) names that the Samba Containers may use.
    The cephx keys for each user in the list will automatically be added to
    the keyring in the container.


Configuring an SMB Service
--------------------------

.. warning::

   A Manager module for smb is under active development. Once that module
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
in the Ceph cluster. There are two ways to store the configuration
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

A non-exhaustive list of important limitations for the smb service follows:

* DNS is a critical component of Active Directory. If one is configuring the
  smb service for domain membership, either the ceph host node must be
  configured so that it can resolve the Active Directory (AD) domain or the
  ``custom_dns`` option may be used. In both cases DNS hosts for the AD domain
  must still be reachable from whatever network segment the ceph cluster is on.
* Proper clustering/high-availability/"transparent state migration" is not yet
  supported. If a placement causes more than service to be created these
  services will act independently and may lead to unexpected behavior if clients
  access the same files at once.
* Services must bind to port 445. Running multiple smb services on the same node
  is not yet supported and will trigger a port-in-use conflict.
