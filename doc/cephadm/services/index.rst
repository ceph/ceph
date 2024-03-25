==================
Service Management
==================

A service is a group of daemons configured together. See these chapters
for details on individual services:

.. toctree::
    :maxdepth: 1

    mon
    mgr
    osd
    rgw
    mds
    nfs
    iscsi
    custom-container
    monitoring
    snmp-gateway
    tracing
    smb

Service Status
==============


To see the status of one
of the services running in the Ceph cluster, do the following:

#. Use the command line to print a list of services.
#. Locate the service whose status you want to check.
#. Print the status of the service.

The following command prints a list of services known to the orchestrator. To
limit the output to services only on a specified host, use the optional
``--host`` parameter. To limit the output to services of only a particular
type, use the optional ``--type`` parameter (mon, osd, mgr, mds, rgw):

   .. prompt:: bash #

     ceph orch ls [--service_type type] [--service_name name] [--export] [--format f] [--refresh]

Discover the status of a particular service or daemon:

   .. prompt:: bash #

     ceph orch ls --service_type type --service_name <name> [--refresh]

To export the service specifications knows to the orchestrator, run the following command.

   .. prompt:: bash #

     ceph orch ls --export

The service specifications exported with this command will be exported as yaml
and that yaml can be used with the ``ceph orch apply -i`` command.

For information about retrieving the specifications of single services (including examples of commands), see :ref:`orchestrator-cli-service-spec-retrieve`.

Daemon Status
=============

A daemon is a systemd unit that is running and part of a service.

To see the status of a daemon, do the following:

#. Print a list of all daemons known to the orchestrator.
#. Query the status of the target daemon.

First, print a list of all daemons known to the orchestrator:

   .. prompt:: bash #

    ceph orch ps [--hostname host] [--daemon_type type] [--service_name name] [--daemon_id id] [--format f] [--refresh]

Then query the status of a particular service instance (mon, osd, mds, rgw).
For OSDs the id is the numeric OSD ID. For MDS services the id is the file
system name:

   .. prompt:: bash #

    ceph orch ps --daemon_type osd --daemon_id 0

.. note::
   The output of the command ``ceph orch ps`` may not reflect the current status of the daemons. By default,
   the status is updated every 10 minutes. This interval can be shortened by modifying the ``mgr/cephadm/daemon_cache_timeout``
   configuration variable (in seconds) e.g: ``ceph config set mgr mgr/cephadm/daemon_cache_timeout 60`` would reduce the refresh
   interval to one minute. The information is updated every ``daemon_cache_timeout`` seconds unless the ``--refresh`` option
   is used. This option would trigger a request to refresh the information, which may take some time depending on the size of
   the cluster. In general ``REFRESHED`` value indicates how recent the information displayed by ``ceph orch ps`` and similar
   commands is.

.. _orchestrator-cli-service-spec:

Service Specification
=====================

A *Service Specification* is a data structure that is used to specify the
deployment of services. In addition to parameters such as `placement` or
`networks`, the user can set initial values of service configuration parameters
by means of the `config` section. For each param/value configuration pair,
cephadm calls the following command to set its value:

   .. prompt:: bash #

    ceph config set <service-name> <param> <value>

cephadm raises health warnings in case invalid configuration parameters are
found in the spec (`CEPHADM_INVALID_CONFIG_OPTION`) or if any error while
trying to apply the new configuration option(s) (`CEPHADM_FAILED_SET_OPTION`).

Here is an example of a service specification in YAML:

.. code-block:: yaml

    service_type: rgw
    service_id: realm.zone
    placement:
      hosts:
        - host1
        - host2
        - host3
    config:
      param_1: val_1
      ...
      param_N: val_N
    unmanaged: false
    networks:
    - 192.169.142.0/24
    spec:
      # Additional service specific attributes.

In this example, the properties of this service specification are:

.. py:currentmodule:: ceph.deployment.service_spec

.. autoclass:: ServiceSpec
   :members:

Each service type can have additional service-specific properties.

Service specifications of type ``mon``, ``mgr``, and the monitoring
types do not require a ``service_id``.

A service of type ``osd`` is described in :ref:`drivegroups`

Many service specifications can be applied at once using ``ceph orch apply -i``
by submitting a multi-document YAML file::

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
following these instructions:

   .. prompt:: bash #

    ceph orch ls --service-name rgw.<realm>.<zone> --export > rgw.<realm>.<zone>.yaml
    ceph orch ls --service-type mgr --export > mgr.yaml
    ceph orch ls --export > cluster.yaml

The Specification can then be changed and re-applied as above.

Updating Service Specifications
-------------------------------

The Ceph Orchestrator maintains a declarative state of each
service in a ``ServiceSpec``. For certain operations, like updating
the RGW HTTP port, we need to update the existing
specification.

1. List the current ``ServiceSpec``:

   .. prompt:: bash #

    ceph orch ls --service_name=<service-name> --export > myservice.yaml

2. Update the yaml file:

   .. prompt:: bash #

    vi myservice.yaml

3. Apply the new ``ServiceSpec``:

   .. prompt:: bash #

    ceph orch apply -i myservice.yaml [--dry-run]

.. _orchestrator-cli-placement-spec:

Daemon Placement
================

For the orchestrator to deploy a *service*, it needs to know where to deploy
*daemons*, and how many to deploy.  This is the role of a placement
specification.  Placement specifications can either be passed as command line arguments
or in a YAML files.

.. note::

   cephadm will not deploy daemons on hosts with the ``_no_schedule`` label; see :ref:`cephadm-special-host-labels`.

.. note::
   The **apply** command can be confusing. For this reason, we recommend using
   YAML specifications.

   Each ``ceph orch apply <service-name>`` command supersedes the one before it.
   If you do not use the proper syntax, you will clobber your work
   as you go.

   For example:

   .. prompt:: bash #

        ceph orch apply mon host1
        ceph orch apply mon host2
        ceph orch apply mon host3

   This results in only one host having a monitor applied to it: host 3.

   (The first command creates a monitor on host1. Then the second command
   clobbers the monitor on host1 and creates a monitor on host2. Then the
   third command clobbers the monitor on host2 and creates a monitor on
   host3. In this scenario, at this point, there is a monitor ONLY on
   host3.)

   To make certain that a monitor is applied to each of these three hosts,
   run a command like this:

   .. prompt:: bash #

     ceph orch apply mon "host1,host2,host3"

   There is another way to apply monitors to multiple hosts: a ``yaml`` file
   can be used. Instead of using the "ceph orch apply mon" commands, run a
   command of this form:

   .. prompt:: bash #

      ceph orch apply -i file.yaml

   Here is a sample **file.yaml** file

   .. code-block:: yaml

        service_type: mon
        placement:
          hosts:
           - host1
           - host2
           - host3

Explicit placements
-------------------

Daemons can be explicitly placed on hosts by simply specifying them:

   .. prompt:: bash #

    ceph orch apply prometheus --placement="host1 host2 host3"

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      hosts:
        - host1
        - host2
        - host3

MONs and other services may require some enhanced network specifications:

   .. prompt:: bash #

    ceph orch daemon add mon --placement="myhost:[v2:1.2.3.4:3300,v1:1.2.3.4:6789]=name"

where ``[v2:1.2.3.4:3300,v1:1.2.3.4:6789]`` is the network address of the monitor
and ``=name`` specifies the name of the new monitor.

.. _orch-placement-by-labels:

Placement by labels
-------------------

Daemon placement can be limited to hosts that match a specific label. To set
a label ``mylabel`` to the appropriate hosts, run this command:

  .. prompt:: bash #

    ceph orch host label add *<hostname>* mylabel

  To view the current hosts and labels, run this command:

  .. prompt:: bash #

    ceph orch host ls

  For example:

  .. prompt:: bash #

    ceph orch host label add host1 mylabel
    ceph orch host label add host2 mylabel
    ceph orch host label add host3 mylabel
    ceph orch host ls

  .. code-block:: bash

    HOST   ADDR   LABELS  STATUS
    host1         mylabel
    host2         mylabel
    host3         mylabel
    host4
    host5

Now, Tell cephadm to deploy daemons based on the label by running
this command:

   .. prompt:: bash #

    ceph orch apply prometheus --placement="label:mylabel"

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      label: "mylabel"

* See :ref:`orchestrator-host-labels`

Placement by pattern matching
-----------------------------

Daemons can be placed on hosts using a host pattern as well.
By default, the host pattern is matched using fnmatch which supports
UNIX shell-style wildcards (see https://docs.python.org/3/library/fnmatch.html):

   .. prompt:: bash #

    ceph orch apply prometheus --placement='myhost[1-3]'

Or in YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      host_pattern: "myhost[1-3]"

To place a service on *all* hosts, use ``"*"``:

   .. prompt:: bash #

    ceph orch apply node-exporter --placement='*'

Or in YAML:

.. code-block:: yaml

    service_type: node-exporter
    placement:
      host_pattern: "*"

The host pattern also has support for using a regex. To use a regex, you
must either add "regex: " to the start of the pattern when using the
command line, or specify a ``pattern_type`` field to be "regex"
when using YAML.

On the command line:

.. prompt:: bash #

 ceph orch apply prometheus --placement='regex:FOO[0-9]|BAR[0-9]'

In YAML:

.. code-block:: yaml

    service_type: prometheus
    placement:
      host_pattern:
        pattern: 'FOO[0-9]|BAR[0-9]'
        pattern_type: regex

Changing the number of daemons
------------------------------

By specifying ``count``, only the number of daemons specified will be created:

   .. prompt:: bash #

    ceph orch apply prometheus --placement=3

To deploy *daemons* on a subset of hosts, specify the count:

   .. prompt:: bash #

    ceph orch apply prometheus --placement="2 host1 host2 host3"

If the count is bigger than the amount of hosts, cephadm deploys one per host:

   .. prompt:: bash #

    ceph orch apply prometheus --placement="3 host1 host2"

The command immediately above results in two Prometheus daemons.

YAML can also be used to specify limits, in the following way:

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 3

YAML can also be used to specify limits on hosts:

.. code-block:: yaml

    service_type: prometheus
    placement:
      count: 2
      hosts:
        - host1
        - host2
        - host3

.. _cephadm_co_location:

Co-location of daemons
----------------------

Cephadm supports the deployment of multiple daemons on the same host:

.. code-block:: yaml

    service_type: rgw
    placement:
      label: rgw
      count_per_host: 2

The main reason for deploying multiple daemons per host is an additional
performance benefit for running multiple RGW and MDS daemons on the same host.

See also: 

* :ref:`cephadm_mgr_co_location`.
* :ref:`cephadm-rgw-designated_gateways`.

This feature was introduced in Pacific.

Algorithm description
---------------------

Cephadm's declarative state consists of a list of service specifications
containing placement specifications.

Cephadm continually compares a list of daemons actually running in the cluster
against the list in the service specifications. Cephadm adds new daemons and
removes old daemons as necessary in order to conform to the service
specifications.

Cephadm does the following to maintain compliance with the service
specifications.

Cephadm first selects a list of candidate hosts. Cephadm seeks explicit host
names and selects them. If cephadm finds no explicit host names, it looks for
label specifications. If no label is defined in the specification, cephadm
selects hosts based on a host pattern. If no host pattern is defined, as a last
resort, cephadm selects all known hosts as candidates.

Cephadm is aware of existing daemons running services and tries to avoid moving
them.

Cephadm supports the deployment of a specific amount of services.
Consider the following service specification:

.. code-block:: yaml

    service_type: mds
    service_name: myfs
    placement:
      count: 3
      label: myfs

This service specification instructs cephadm to deploy three daemons on hosts
labeled ``myfs`` across the cluster.

If there are fewer than three daemons deployed on the candidate hosts, cephadm
randomly chooses hosts on which to deploy new daemons.

If there are more than three daemons deployed on the candidate hosts, cephadm
removes existing daemons.

Finally, cephadm removes daemons on hosts that are outside of the list of
candidate hosts.

.. note::

   There is a special case that cephadm must consider.

   If there are fewer hosts selected by the placement specification than
   demanded by ``count``, cephadm will deploy only on the selected hosts.

.. _cephadm-extra-container-args:

Extra Container Arguments
=========================

.. warning:: 
  The arguments provided for extra container args are limited to whatever arguments are available for
  a `run` command from whichever container engine you are using. Providing any arguments the `run`
  command does not support (or invalid values for arguments) will cause the daemon to fail to start.

.. note::

  For arguments passed to the process running inside the container rather than the for
  the container runtime itself, see :ref:`cephadm-extra-entrypoint-args`


Cephadm supports providing extra miscellaneous container arguments for
specific cases when they may be necessary. For example, if a user needed
to limit the amount of cpus their mon daemons make use of they could apply
a spec like

.. code-block:: yaml

    service_type: mon
    service_name: mon
    placement:
      hosts:
        - host1
        - host2
        - host3
    extra_container_args:
      - "--cpus=2"

which would cause each mon daemon to be deployed with `--cpus=2`.

There are two ways to express arguments in the ``extra_container_args`` list.
To start, an item in the list can be a string. When passing an argument
as a string and the string contains spaces, Cephadm will automatically split it
into multiple arguments. For example, ``--cpus 2`` would become ``["--cpus",
"2"]`` when processed. Example:

.. code-block:: yaml

    service_type: mon
    service_name: mon
    placement:
      hosts:
        - host1
        - host2
        - host3
    extra_container_args:
      - "--cpus 2"

As an alternative, an item in the list can be an object (mapping) containing
the required key "argument" and an optional key "split". The value associated
with the ``argument`` key must be a single string. The value associated with
the ``split`` key is a boolean value. The ``split`` key explicitly controls if
spaces in the argument value cause the value to be split into multiple
arguments. If ``split`` is true then Cephadm will automatically split the value
into multiple arguments.  If ``split`` is false then spaces in the value will
be retained in the argument.  The default, when ``split`` is not provided, is
false. Examples:

.. code-block:: yaml

    service_type: mon
    service_name: mon
    placement:
      hosts:
        - tiebreaker
    extra_container_args:
      # No spaces, always treated as a single argument
      - argument: "--timout=3000"
      # Splitting explicitly disabled, one single argument
      - argument: "--annotation=com.example.name=my favorite mon"
        split: false
      # Splitting explicitly enabled, will become two arguments
      - argument: "--cpuset-cpus 1-3,7-11"
        split: true
      # Splitting implicitly disabled, one single argument
      - argument: "--annotation=com.example.note=a simple example"

Mounting Files with Extra Container Arguments
---------------------------------------------

A common use case for extra container arguments is to mount additional
files within the container. Older versions of Ceph did not support spaces
in arguments and therefore the examples below apply to the widest range
of Ceph versions.

.. code-block:: yaml

    extra_container_args:
      - "-v"
      - "/absolute/file/path/on/host:/absolute/file/path/in/container"

For example:

.. code-block:: yaml

    extra_container_args:
      - "-v"
      - "/opt/ceph_cert/host.cert:/etc/grafana/certs/cert_file:ro"

.. _cephadm-extra-entrypoint-args:

Extra Entrypoint Arguments
==========================

.. note::

  For arguments intended for the container runtime rather than the process inside
  it, see :ref:`cephadm-extra-container-args`

Similar to extra container args for the container runtime, Cephadm supports
appending to args passed to the entrypoint process running
within a container. For example, to set the collector textfile directory for
the node-exporter service , one could apply a service spec like

.. code-block:: yaml

  service_type: node-exporter
  service_name: node-exporter
  placement:
    host_pattern: '*'
  extra_entrypoint_args:
    - "--collector.textfile.directory=/var/lib/node_exporter/textfile_collector2"

There are two ways to express arguments in the ``extra_entrypoint_args`` list.
To start, an item in the list can be a string. When passing an argument as a
string and the string contains spaces, cephadm will automatically split it into
multiple arguments. For example, ``--debug_ms 10`` would become
``["--debug_ms", "10"]`` when processed. Example:

.. code-block:: yaml

    service_type: mon
    service_name: mon
    placement:
      hosts:
        - host1
        - host2
        - host3
    extra_entrypoint_args:
      - "--debug_ms 2"

As an alternative, an item in the list can be an object (mapping) containing
the required key "argument" and an optional key "split". The value associated
with the ``argument`` key must be a single string. The value associated with
the ``split`` key is a boolean value. The ``split`` key explicitly controls if
spaces in the argument value cause the value to be split into multiple
arguments. If ``split`` is true then cephadm will automatically split the value
into multiple arguments.  If ``split`` is false then spaces in the value will
be retained in the argument.  The default, when ``split`` is not provided, is
false. Examples:

.. code-block:: yaml

    # An theoretical data migration service
    service_type: pretend
    service_name: imagine1
    placement:
      hosts:
        - host1
    extra_entrypoint_args:
      # No spaces, always treated as a single argument
      - argument: "--timout=30m"
      # Splitting explicitly disabled, one single argument
      - argument: "--import=/mnt/usb/My Documents"
        split: false
      # Splitting explicitly enabled, will become two arguments
      - argument: "--tag documents"
        split: true
      # Splitting implicitly disabled, one single argument
      - argument: "--title=Imported Documents"


Custom Config Files
===================

Cephadm supports specifying miscellaneous config files for daemons.
To do so, users must provide both the content of the config file and the
location within the daemon's container at which it should be mounted. After
applying a YAML spec with custom config files specified and having cephadm
redeploy the daemons for which the config files are specified, these files will
be mounted within the daemon's container at the specified location.

Example service spec:

.. code-block:: yaml

    service_type: grafana
    service_name: grafana
    custom_configs:
      - mount_path: /etc/example.conf
        content: |
          setting1 = value1
          setting2 = value2
      - mount_path: /usr/share/grafana/example.cert
        content: |
          -----BEGIN PRIVATE KEY-----
          V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4gTG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFt
          ZXQsIGNvbnNldGV0dXIgc2FkaXBzY2luZyBlbGl0ciwgc2VkIGRpYW0gbm9udW15
          IGVpcm1vZCB0ZW1wb3IgaW52aWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu
          YSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3Mg
          ZXQgYWNjdXNhbSBldCBqdXN0byBkdW8=
          -----END PRIVATE KEY-----
          -----BEGIN CERTIFICATE-----
          V2VyIGRhcyBsaWVzdCBpc3QgZG9vZi4gTG9yZW0gaXBzdW0gZG9sb3Igc2l0IGFt
          ZXQsIGNvbnNldGV0dXIgc2FkaXBzY2luZyBlbGl0ciwgc2VkIGRpYW0gbm9udW15
          IGVpcm1vZCB0ZW1wb3IgaW52aWR1bnQgdXQgbGFib3JlIGV0IGRvbG9yZSBtYWdu
          YSBhbGlxdXlhbSBlcmF0LCBzZWQgZGlhbSB2b2x1cHR1YS4gQXQgdmVybyBlb3Mg
          ZXQgYWNjdXNhbSBldCBqdXN0byBkdW8=
          -----END CERTIFICATE-----

To make these new config files actually get mounted within the
containers for the daemons

.. prompt:: bash

  ceph orch redeploy <service-name>

For example:

.. prompt:: bash

  ceph orch redeploy grafana

.. _orch-rm:

Removing a Service
==================

In order to remove a service including the removal
of all daemons of that service, run

.. prompt:: bash

  ceph orch rm <service-name>

For example:

.. prompt:: bash

  ceph orch rm rgw.myrgw

.. _cephadm-spec-unmanaged:

Disabling automatic deployment of daemons
=========================================

Cephadm supports disabling the automated deployment and removal of daemons on a
per service basis. The CLI supports two commands for this.

In order to fully remove a service, see :ref:`orch-rm`.

Disabling automatic management of daemons
-----------------------------------------

To disable the automatic management of dameons, set ``unmanaged=True`` in the
:ref:`orchestrator-cli-service-spec` (``mgr.yaml``).

``mgr.yaml``:

.. code-block:: yaml

  service_type: mgr
  unmanaged: true
  placement:
    label: mgr


.. prompt:: bash #

   ceph orch apply -i mgr.yaml

Cephadm also supports setting the unmanaged parameter to true or false
using the ``ceph orch set-unmanaged`` and ``ceph orch set-managed`` commands.
The commands take the service name (as reported in ``ceph orch ls``) as
the only argument. For example,

.. prompt:: bash #

   ceph orch set-unmanaged mon

would set ``unmanaged: true`` for the mon service and

.. prompt:: bash #

   ceph orch set-managed mon

would set ``unmanaged: false`` for the mon service

.. note::

  After you apply this change in the Service Specification, cephadm will no
  longer deploy any new daemons (even if the placement specification matches
  additional hosts).

.. note::

  The "osd" service used to track OSDs that are not tied to any specific
  service spec is special and will always be marked unmanaged. Attempting
  to modify it with ``ceph orch set-unmanaged`` or ``ceph orch set-managed``
  will result in a message ``No service of name osd found. Check "ceph orch ls" for all known services``

Deploying a daemon on a host manually
-------------------------------------

.. note::

  This workflow has a very limited use case and should only be used
  in rare circumstances.

To manually deploy a daemon on a host, follow these steps:

Modify the service spec for a service by getting the
existing spec, adding ``unmanaged: true``, and applying the modified spec.

Then manually deploy the daemon using the following:

   .. prompt:: bash #

     ceph orch daemon add <daemon-type>  --placement=<placement spec>

For example :

   .. prompt:: bash #

     ceph orch daemon add mgr --placement=my_host

.. note::

  Removing ``unmanaged: true`` from the service spec will
  enable the reconciliation loop for this service and will
  potentially lead to the removal of the daemon, depending
  on the placement spec.

Removing a daemon from a host manually
--------------------------------------

To manually remove a daemon, run a command of the following form:

   .. prompt:: bash #

     ceph orch daemon rm <daemon name>... [--force]

For example:

   .. prompt:: bash #

     ceph orch daemon rm mgr.my_host.xyzxyz

.. note::

  For managed services (``unmanaged=False``), cephadm will automatically
  deploy a new daemon a few seconds later.

See also
--------

* See :ref:`cephadm-osd-declarative` for special handling of unmanaged OSDs.
* See also :ref:`cephadm-pause`
