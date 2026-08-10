===========
MON Service
===========

.. _deploy_additional_monitors:

Deploying additional Monitors 
=============================

A typical Ceph cluster has three or five Monitor daemons that are spread
across different hosts.  We recommend deploying five Monitors if there are
five or more nodes in your cluster.

.. _CIDR: https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing#CIDR_notation

Ceph deploys Monitor daemons automatically as the cluster grows and Ceph
scales back Monitor daemons automatically as the cluster shrinks. The
smooth execution of this automatic growing and shrinking depends upon
proper subnet configuration.

The cephadm bootstrap procedure assigns the first Monitor daemon in the
cluster to a particular subnet. ``cephadm`` designates that subnet as the
default subnet of the cluster. New Monitor daemons will be assigned by
default to that subnet unless cephadm is instructed to do otherwise. 

If all of the Ceph Monitor daemons in your cluster are in the same subnet,
manual administration of the Ceph Monitor daemons is not necessary.
``cephadm`` will automatically add up to five Monitors to the subnet, as
needed, as new hosts are added to the cluster.

By default, cephadm will deploy 5 daemons on arbitrary hosts. See
:ref:`orchestrator-cli-placement-spec` for details of specifying
the placement of daemons.


.. _mon-service-spec:

Service Specification
=====================

The Monitor service can be deployed or updated via a YAML specification
file with ``ceph orch apply -i``. The following is a complete example
showing all parameters available as of August 2026:

.. code-block:: yaml

    service_type: mon
    placement:
      hosts:
        - host1
        - host2
        - host3
      count: 5
      label: mon
      host_pattern: "mon*"
      count_per_host: 1
    config:
      mon_warn_on_pool_no_redundancy: "false"
    unmanaged: false
    preview_only: false
    networks:
    - 10.1.2.0/24
    extra_container_args:
      - "--cpus=2"
    extra_entrypoint_args:
      - "--mon-data=/var/lib/ceph/mon"
    custom_configs:
      - mount_path: /etc/custom/mon.conf
        content: |
          [custom]
          setting = value
    spec:
      crush_locations:
        host1:
          - datacenter=a
        host2:
          - datacenter=b
          - rack=2
        host3:
          - datacenter=a

.. note::

   The ``placement`` fields (``hosts``, ``label``, ``host_pattern``,
   ``count``, ``count_per_host``) are mutually exclusive in certain
   combinations. See :ref:`orchestrator-cli-placement-spec` for details.

.. note::

   The ``mon`` service type does **not** use a ``service_id``.

Top-level Parameters
--------------------

The following parameters are available at the top level of the Monitor service
specification:

``service_type``
    **Required.** Must be ``mon``.

``placement``
    Specifies where and how many Monitor daemons to deploy. See
    `Placement Parameters`_ below for available sub-fields. If omitted,
    cephadm uses its default placement strategy (up to 5 Monitors).

``config``
    A mapping of Ceph configuration option names to values. These are
    applied via ``ceph config set mon <key> <value>`` when the spec is
    applied. Example:

    .. code-block:: yaml

        config:
          mon_warn_on_pool_no_redundancy: "false"
          mon_osd_full_ratio: "0.95"

``unmanaged``
    Boolean (default: ``false``). When ``true``, cephadm will not
    automatically deploy, remove, or reconfig Monitor daemons. Daemons
    must be manually added with ``ceph orch daemon add mon``.

``preview_only``
    Boolean (default: ``false``). When ``true``, changes are previewed
    but not applied.

``networks``
    A list of IP networks in CIDR notation (e.g., ``10.1.2.0/24``) to
    which Monitor daemons should bind. If specified, Monitors will only
    be deployed on hosts with IP addresses matching one of the listed
    networks.

    .. code-block:: yaml

        networks:
        - 10.1.2.0/24
        - 192.168.1.0/24

``extra_container_args``
    A list of additional arguments passed to the container runtime
    (podman/docker) when starting the Monitor container. Each entry can
    be a plain string or an object with ``argument`` and ``split`` fields.

    .. code-block:: yaml

        extra_container_args:
          - "--cpus=2"
          - argument: "--memory=4g"
            split: false

``extra_entrypoint_args``
    A list of additional arguments appended to the Monitor daemon's
    entrypoint command. Each entry can be a plain string or an object with
    ``argument`` and ``split`` fields.

    .. code-block:: yaml

        extra_entrypoint_args:
          - "--mon-data=/var/lib/ceph/mon"

``custom_configs``
    A list of custom configuration files to mount inside the Monitor
    container. Each entry has ``mount_path`` (the path inside the
    container) and ``content`` (the file contents).

    .. code-block:: yaml

        custom_configs:
          - mount_path: /etc/custom/mon.conf
            content: |
              [custom]
              setting = value

Placement Parameters
--------------------

The ``placement`` section specifies the number of Monitors to deploy and
the hosts on which they may be placed. The following sub-fields are
available:

``hosts``
    A list of hostnames where Monitors should be deployed. Each entry can
    be a simple hostname or an extended format with network and name:

    - ``hostname`` — deploy on this host
    - ``hostname:network`` — deploy on this host, binding to the specified
      IP or network
    - ``hostname=name`` — deploy with a custom daemon name suffix

    .. code-block:: yaml

        placement:
          hosts:
            - host1
            - host2:10.1.2.0/24
            - host3=custom-name

``count``
    Integer (>= 1). The total number of Monitor daemons to deploy.
    Cephadm will choose hosts automatically if ``hosts`` is not specified.
    Mutually exclusive with ``count_per_host``.

``count_per_host``
    Integer (>= 1). Number of daemons to deploy per matching host.
    Requires ``label``, ``hosts``, or ``host_pattern`` to be set.
    Mutually exclusive with ``count``.

``label``
    A host label string. Monitor daemons will be deployed on all hosts
    that have this label assigned. Mutually exclusive with ``hosts``.

    .. code-block:: yaml

        placement:
          label: mon

``host_pattern``
    An `fnmatch <https://docs.python.org/3/library/fnmatch.html>`_-style
    pattern or regex to select hosts by name. Mutually exclusive with
    ``hosts``. Can be a string (fnmatch) or an object with ``pattern``
    and ``pattern_type`` fields:

    .. code-block:: yaml

        # fnmatch pattern (default)
        placement:
          host_pattern: "mon*"

        # regex pattern
        placement:
          host_pattern:
            pattern: "mon[0-9]+"
            pattern_type: regex

``mon``-Specific Parameters (``spec`` section)
-----------------------------------------------

The following parameters are specific to the ``mon`` service and are placed
under the ``spec:`` section:

``crush_locations``
    A mapping of hostnames to lists of CRUSH location strings. Each
    CRUSH location must be in the format ``<bucket_type>=<location>``
    (e.g., ``datacenter=dc1``, ``rack=rack2``).

    When cephadm deploys a monitor on a host listed here, it sets the
    CRUSH location via ``--set-crush-location``. If multiple CRUSH
    locations are specified for a host, the first is used at deploy time
    and additional locations are applied via ``ceph mon set_location``.

    .. code-block:: yaml

        spec:
          crush_locations:
            host1:
              - datacenter=a
            host2:
              - datacenter=b
              - rack=2
            host3:
              - datacenter=a

    .. note::

       Setting the CRUSH location in the spec is the recommended way of
       replacing tiebreaker Monitor daemons, as they require having a
       location set when they are added. Tiebreaker Monitors are only
       relevant for stretch mode clusters; see :ref:`stretch_mode`.

    .. note::

       Monitor daemons will only get the ``--set-crush-location`` flag
       set when cephadm actually deploys them. If a spec is applied that
       includes a CRUSH location for a Monitor that is already deployed,
       the flag may not be set until a ``redeploy`` command is issued.

Complete Minimal Examples
-------------------------

Deploy 5 Monitors on labeled hosts:

.. code-block:: yaml

    service_type: mon
    placement:
      count: 5
      label: mon

Deploy Monitors on specific hosts with network binding:

.. code-block:: yaml

    service_type: mon
    placement:
      hosts:
        - host1:10.1.2.0/24
        - host2:10.1.2.0/24
        - host3:10.1.2.0/24
    networks:
    - 10.1.2.0/24

Deploy Monitors with CRUSH locations for stretch mode:

.. code-block:: yaml

    service_type: mon
    placement:
      count: 5
    spec:
      crush_locations:
        host1:
          - datacenter=a
        host2:
          - datacenter=a
        host3:
          - datacenter=b
        host4:
          - datacenter=b
        host5:
          - datacenter=c


Designating a Particular Subnet for Monitors
--------------------------------------------

To designate a particular IP subnet for use by Ceph Monitor daemons, use a
command of the following form, including the subnet's address in `CIDR`_
format (e.g., ``10.1.2.0/24``):

  .. prompt:: bash #

     ceph config set mon public_network <mon-cidr-network>

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24

Cephadm deploys new Monitor daemons only on hosts that have IP addresses in
the designated subnet.

You can also specify two public networks by using a list of networks:

  .. prompt:: bash #

     ceph config set mon public_network <mon-cidr-network1>,<mon-cidr-network2>

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24,192.168.0.1/24


Deploying Monitors on a Particular Network 
------------------------------------------

You can explicitly specify the IP address or CIDR network for each Monitor and
control where each Monitor is placed.  To disable automated Monitor deployment,
run this command:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged

  To deploy each additional Monitor:

  .. prompt:: bash #

    ceph orch daemon add mon <host1:ip-or-network1>

  For example, to deploy a second Monitor on ``newhost1`` using an IP
  address ``10.1.2.123`` and a third Monitor on ``newhost2`` in
  network ``10.1.2.0/24``, run the following commands:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged
    ceph orch daemon add mon newhost1:10.1.2.123
    ceph orch daemon add mon newhost2:10.1.2.0/24

  Now, enable automatic placement of daemons

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3" --dry-run

  See :ref:`orchestrator-cli-placement-spec` for details of specifying
  the placement of daemons.

  Finally apply this new placement by dropping ``--dry-run``

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3"


Moving Monitors to a Different Network
--------------------------------------

To move Monitors to a new network, deploy new Monitors on the new network and
subsequently remove Monitors from the old network. It is not advised to
modify and inject the ``monmap`` manually.

First, disable the automated placement of daemons:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged

To deploy each additional Monitor:

  .. prompt:: bash #

    ceph orch daemon add mon <newhost1:ip-or-network1>

For example, to deploy a second Monitor on ``newhost1`` using an IP
address ``10.1.2.123`` and a third Monitor on ``newhost2`` in
network ``10.1.2.0/24``, run the following commands:

  .. prompt:: bash #

    ceph orch apply mon --unmanaged
    ceph orch daemon add mon newhost1:10.1.2.123
    ceph orch daemon add mon newhost2:10.1.2.0/24

  Subsequently remove Monitors from the old network:

  .. prompt:: bash #

    ceph orch daemon rm *mon.<oldhost1>*

  Update the ``public_network``:

  .. prompt:: bash #

     ceph config set mon public_network <mon-cidr-network>

  For example:

  .. prompt:: bash #

     ceph config set mon public_network 10.1.2.0/24

  Now, enable automatic placement of daemons

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3" --dry-run

  See :ref:`orchestrator-cli-placement-spec` for details of specifying
  the placement of daemons.

  Finally apply this new placement by dropping ``--dry-run``

  .. prompt:: bash #

    ceph orch apply mon --placement="newhost1,newhost2,newhost3" 


Setting Crush Locations for Monitors
------------------------------------

Cephadm supports setting CRUSH locations for mon daemons
using the mon service spec. The CRUSH locations are set
by hostname. When cephadm deploys a mon on a host that matches
a hostname specified in the CRUSH locations, it will add
``--set-crush-location <CRUSH-location>`` where the CRUSH location
is the first entry in the list of CRUSH locations for that
host. If multiple CRUSH locations are set for one host, cephadm
will attempt to set the additional locations using the
"ceph mon set_location" command.

See the ``crush_locations`` parameter in :ref:`mon-service-spec` for the
full specification reference.

.. note::

   Setting the CRUSH location in the spec is the recommended way of
   replacing tiebreaker mon daemons, as they require having a location
   set when they are added.

.. note::

   Tiebreaker mon daemons are a part of stretch mode clusters. For more
   info on stretch mode clusters see :ref:`stretch_mode`

Example syntax for setting the CRUSH locations:

.. code-block:: yaml

    service_type: mon
    service_name: mon
    placement:
      count: 5
    spec:
      crush_locations:
        host1:
        - datacenter=a
        host2:
        - datacenter=b
        - rack=2
        host3:
        - datacenter=a

.. note::

   Sometimes, based on the timing of mon daemons being admitted to the mon
   quorum, cephadm may fail to set the CRUSH location for some mon daemons
   when multiple locations are specified. In this case, the recommended
   action is to re-apply the same mon spec to retrigger the service action.

.. note::

   Mon daemons will only get the ``--set-crush-location`` flag set when cephadm
   actually deploys them. This means if a spec is applied that includes a CRUSH
   location for a mon that is already deployed, the flag may not be set until
   a redeploy command is issued for that mon daemon.


Further Reading
===============

* :ref:`rados-operations`
* :ref:`rados-troubleshooting-mon`
* :ref:`cephadm-restore-quorum`

