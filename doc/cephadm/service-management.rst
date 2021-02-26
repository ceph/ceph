==================
Service Management
==================

Service Status
==============

A service is a group of daemons that are configured together.

Print a list of services known to the orchestrator. The list can be limited to
services on a particular host with the optional --host parameter and/or
services of a particular type via optional --type parameter
(mon, osd, mgr, mds, rgw):

::

    ceph orch ls [--service_type type] [--service_name name] [--export] [--format f] [--refresh]

Discover the status of a particular service or daemons::

    ceph orch ls --service_type type --service_name <name> [--refresh]

Export the service specs known to the orchestrator as yaml in format
that is compatible to ``ceph orch apply -i``::

    ceph orch ls --export

For examples about retrieving specs of single services see :ref:`orchestrator-cli-service-spec-retrieve`.

Daemon Status
=============

A daemon is a running systemd unit and is part of a service.

Print a list of all daemons known to the orchestrator::

    ceph orch ps [--hostname host] [--daemon_type type] [--service_name name] [--daemon_id id] [--format f] [--refresh]

Query the status of a particular service instance (mon, osd, mds, rgw).  For OSDs
the id is the numeric OSD ID, for MDS services it is the file system name::

    ceph orch ps --daemon_type osd --daemon_id 0
    
.. _orchestrator-cli-service-spec:

Service Specification
=====================

A *Service Specification* is a data structure
to specify the deployment of services.  For example in YAML:

.. code-block:: yaml

    service_type: rgw
    service_id: realm.zone
    placement:
      hosts:
        - host1
        - host2
        - host3
    unmanaged: false
    ...

where the properties of a service specification are:

* ``service_type``
    The type of the service. Needs to be either a Ceph
    service (``mon``, ``crash``, ``mds``, ``mgr``, ``osd`` or
    ``rbd-mirror``), a gateway (``nfs`` or ``rgw``), part of the
    monitoring stack (``alertmanager``, ``grafana``, ``node-exporter`` or
    ``prometheus``) or (``container``) for custom containers.
* ``service_id``
    The name of the service.
* ``placement``
    See :ref:`orchestrator-cli-placement-spec`.
* ``unmanaged``
    If set to ``true``, the orchestrator will not deploy nor
    remove any daemon associated with this service. Placement and all other
    properties will be ignored. This is useful, if this service should not
    be managed temporarily. For cephadm, See :ref:`cephadm-spec-unmanaged`

Each service type can have additional service specific properties.

Service specifications of type ``mon``, ``mgr``, and the monitoring
types do not require a ``service_id``.

A service of type ``osd`` is described in :ref:`drivegroups`

Many service specifications can be applied at once using
``ceph orch apply -i`` by submitting a multi-document YAML file::

    cat <<EOF | ceph orch apply -i -
    service_type: mon
    placement:
      host_pattern: "mon*"
    ---
    service_type: mgr
    placement:
      host_pattern: "mgr*"
    ---
    service_type: osd
    service_id: default_drive_group
    placement:
      host_pattern: "osd*"
    data_devices:
      all: true
    EOF

.. _orchestrator-cli-service-spec-retrieve:

Retrieving the running Service Specification
--------------------------------------------

If the services have been started via ``ceph orch apply...``, then directly changing
the Services Specification is complicated. Instead of attempting to directly change
the Services Specification, we suggest exporting the running Service Specification by
following these instructions::
    
    ceph orch ls --service-name rgw.<realm>.<zone> --export > rgw.<realm>.<zone>.yaml
    ceph orch ls --service-type mgr --export > mgr.yaml
    ceph orch ls --export > cluster.yaml

The Specification can then be changed and re-applied as above.

.. _orchestrator-cli-placement-spec:

Placement Specification
=======================

For the orchestrator to deploy a *service*, it needs to know where to deploy
*daemons*, and how many to deploy.  This is the role of a placement
specification.  Placement specifications can either be passed as command line arguments
or in a YAML files.

Explicit placements
-------------------

Daemons can be explicitly placed on hosts by simply specifying them::

    orch apply prometheus --placement="host1 host2 host3"

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      hosts:
        - host1
        - host2
        - host3

MONs and other services may require some enhanced network specifications::

  orch daemon add mon --placement="myhost:[v2:1.2.3.4:3300,v1:1.2.3.4:6789]=name"

where ``[v2:1.2.3.4:3300,v1:1.2.3.4:6789]`` is the network address of the monitor
and ``=name`` specifies the name of the new monitor.

.. _orch-placement-by-labels:

Placement by labels
-------------------

Daemons can be explicitly placed on hosts that match a specific label::

    orch apply prometheus --placement="label:mylabel"

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      label: "mylabel"

* See :ref:`orchestrator-host-labels`

Placement by pattern matching
-----------------------------

Daemons can be placed on hosts as well::

    orch apply prometheus --placement='myhost[1-3]'

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      host_pattern: "myhost[1-3]"

To place a service on *all* hosts, use ``"*"``::

    orch apply node-exporter --placement='*'

Or in YAML:

.. code-block:: yaml

    service_type: node-exporter
    placement:
      host_pattern: "*"


Setting a limit
---------------

By specifying ``count``, only that number of daemons will be created::

    orch apply prometheus --placement=3

To deploy *daemons* on a subset of hosts, also specify the count::

    orch apply prometheus --placement="2 host1 host2 host3"

If the count is bigger than the amount of hosts, cephadm deploys one per host::

    orch apply prometheus --placement="3 host1 host2"

results in two Prometheus daemons.

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 3

Or with hosts:

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 2
      hosts:
        - host1
        - host2
        - host3

Updating Service Specifications
===============================

The Ceph Orchestrator maintains a declarative state of each
service in a ``ServiceSpec``. For certain operations, like updating
the RGW HTTP port, we need to update the existing
specification.

1. List the current ``ServiceSpec``::

    ceph orch ls --service_name=<service-name> --export > myservice.yaml

2. Update the yaml file::

    vi myservice.yaml

3. Apply the new ``ServiceSpec``::

    ceph orch apply -i myservice.yaml [--dry-run]
    
Deployment of Daemons
=====================

Cephadm uses a declarative state to define the layout of the cluster. This
state consists of a list of service specifications containing placement
specifications (See :ref:`orchestrator-cli-service-spec` ). 

Cephadm constantly compares list of actually running daemons in the cluster
with the desired service specifications and will either add or remove new 
daemons.

First, cephadm will select a list of candidate hosts. It first looks for 
explicit host names and will select those. In case there are no explicit hosts 
defined, cephadm looks for a label specification. If there is no label defined 
in the specification, cephadm will select hosts based on a host pattern. If 
there is no pattern defined, cepham will finally select all known hosts as
candidates.

Then, cephadm will consider existing daemons of this services and will try to
avoid moving any daemons.

Cephadm supports the deployment of a specific amount of services. Let's 
consider a service specification like so:

.. code-block:: yaml

    service_type: mds
    service_name: myfs
    placement:
      count: 3
      label: myfs

This instructs cephadm to deploy three daemons on hosts labeled with
``myfs`` across the cluster.

Then, in case there are less than three daemons deployed on the candidate 
hosts, cephadm will then randomly choose hosts for deploying new daemons.

In case there are more than three daemons deployed, cephadm will remove 
existing daemons.

Finally, cephadm will remove daemons on hosts that are outside of the list of 
candidate hosts.

However, there is a special cases that cephadm needs to consider.

In case the are fewer hosts selected by the placement specification than 
demanded by ``count``, cephadm will only deploy on selected hosts.


.. _cephadm-spec-unmanaged:

Disable automatic deployment of daemons
=======================================

Cephadm supports disabling the automated deployment and removal of daemons per service. In
this case, the CLI supports two commands that are dedicated to this mode. 

To disable the automatic management of dameons, apply
the :ref:`orchestrator-cli-service-spec` with ``unmanaged=True``. 

``mgr.yaml``:

.. code-block:: yaml

  service_type: mgr
  unmanaged: true
  placement:
    label: mgr

.. code-block:: bash

  ceph orch apply -i mgr.yaml

.. note::

  cephadm will no longer deploy any new daemons, if the placement
  specification matches additional hosts.

To manually deploy a daemon on a host, please execute:

.. code-block:: bash

  ceph orch daemon add <daemon-type>  --placement=<placement spec>

For example 

.. code-block:: bash

  ceph orch daemon add mgr --placement=my_host

To manually remove a daemon, please run:

.. code-block:: bash

  ceph orch daemon rm <daemon name>... [--force]

For example 

.. code-block:: bash

  ceph orch daemon rm mgr.my_host.xyzxyz

.. note:: 

  For managed services (``unmanaged=False``), cephadm will automatically
  deploy a new daemon a few seconds later.
    
* See :ref:`cephadm-osd-declarative` for special handling of unmanaged OSDs. 
* See also :ref:`cephadm-pause`
