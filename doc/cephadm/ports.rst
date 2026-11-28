.. _cephadm-ports:

=================
Port Requirements
=================

This page lists the network ports that the daemons of a cephadm-managed
cluster listen on, who connects to each port, and the setting or service
specification field that changes it. Open these ports in any firewall between
the hosts and clients named in the *Connects from* column. All ports are TCP
unless a row says otherwise. The service specification fields named in the
*Set with* column go in the ``spec`` section of the specification.

When a service runs more than one daemon on a host, for example with
``count_per_host``, each additional daemon on that host gets the next port up
from each port of the first daemon, unless the specification sets each
daemon's ports.

cephadm opens many of these ports itself on hosts that run firewalld. See
:ref:`cephadm-ports-firewalld` for what it opens and what it leaves for you to
open. For the messenger port range and ``iptables`` examples, see
:ref:`network-config-ip-tables`.

Ceph Daemons
============

.. list-table::
   :header-rows: 1
   :widths: 15 20 35 30

   * - Daemon
     - Port
     - Connects from
     - Set with
   * - Monitor
     - 3300 (msgr2) and 6789 (msgr1)
     - Clients, every other Ceph daemon, and the other Monitors
     - The Monitor's address vector: for example,
       ``ceph orch daemon add mon <host>:[v2:<ip>:<port>,v1:<ip>:<port>]``,
       or ``--mon-addrv`` for ``cephadm bootstrap``
   * - OSD
     - The first free ports in 6800-7568
     - Clients, Monitors, Managers, MDSes, and other OSDs. If there is a
       cluster network, other OSDs also connect over it: see
       :ref:`cluster-network`.
     - :confval:`ms_bind_port_min` and :confval:`ms_bind_port_max`
   * - Manager
     - The first free ports in 6800-7568
     - Clients and every other Ceph daemon
     - :confval:`ms_bind_port_min` and :confval:`ms_bind_port_max`
   * - MDS
     - The first free ports in 6800-7568
     - CephFS clients, Monitors, Managers, and other MDSes
     - :confval:`ms_bind_port_min` and :confval:`ms_bind_port_max`

An OSD binds several ports, and a daemon that restarts can bind different
ports in the range, so open the whole range on every host that runs an OSD, a
Manager, or an MDS.

Manager and cephadm Endpoints
=============================

.. list-table::
   :header-rows: 1
   :widths: 15 20 35 30

   * - Service
     - Port
     - Connects from
     - Set with
   * - Dashboard Manager module
     - 8443, or 8080 when ``mgr/dashboard/ssl`` is ``false``
     - Browsers and the mgmt-gateway, to the Manager hosts
     - ``mgr/dashboard/ssl_server_port`` or ``mgr/dashboard/server_port``: see
       :ref:`dashboard-host-name-and-port`
   * - ``prometheus`` Manager module
     - 9283. Beginning with the Vampire release, 9284 when
       ``mgr/prometheus/ssl`` is ``true``.
     - Prometheus, to the Manager hosts
     - :confval:`mgr/prometheus/server_port` or
       ``mgr/prometheus/ssl_server_port``
   * - cephadm service discovery
     - 8765
     - Prometheus and the mgmt-gateway, to the active Manager
     - ``mgr/cephadm/service_discovery_port``
   * - cephadm agent endpoint
     - 7150, or the next free port up to 7300
     - cephadm agents and ``node-proxy`` daemons, to the active Manager
     - Not configurable
   * - cephadm agent
     - 4721, or the next free port among the 1000 above it
     - The active Manager, to each host. The agent runs only when
       ``mgr/cephadm/use_agent`` is ``true``, and it is ``false`` by default.
     - ``mgr/cephadm/agent_starting_port``
   * - SSH
     - 22
     - The active Manager, to every host. The active Manager can fail over to
       any host that runs a Manager, so allow SSH from each of them.
     - A ``Port`` line in the SSH configuration that
       ``ceph cephadm set-ssh-config`` sets

Gateways
========

.. list-table::
   :header-rows: 1
   :widths: 15 20 35 30

   * - Service
     - Port
     - Connects from
     - Set with
   * - RGW
     - 80, or 443 when the specification sets ``ssl: true``
     - S3 and Swift clients, and the ingress service if there is one
     - ``rgw_frontend_port``. Beginning with the Vampire release,
       ``rgw_frontend_http_port`` adds a plain HTTP port next to the SSL port.
       A ``radosgw`` daemon that cephadm did not deploy listens on 7480, the
       port in the default value of :confval:`rgw_frontends`.
   * - NFS
     - 2049
     - NFS clients, and the ingress service if there is one
     - ``port``
   * - NFS
     - 9587
     - Prometheus
     - ``monitoring_port``
   * - NFS
     - 31311. Beginning with the Umbrella release.
     - The other NFS daemons of the service, for cluster QoS
     - ``cluster_qos_port``
   * - NFS
     - 20049, over RDMA. Beginning with the Umbrella release, when
       ``enable_rdma`` is ``true``.
     - NFS clients that use RDMA
     - ``rdma_port``
   * - Ingress (``haproxy``)
     - None: the specification must set it
     - Clients of the backend service, at the virtual IP address
     - ``frontend_port``
   * - Ingress (``haproxy``)
     - None: the specification must set it
     - Prometheus, and users of the HAProxy statistics page
     - ``monitor_port``
   * - Ingress (``haproxy``)
     - 1024, for an NFS backend only. Beginning with the Umbrella release.
     - The other ``haproxy`` daemons of the service
     - ``haproxy_peer_communication_port``
   * - Ingress (``keepalived``)
     - VRRP, which is IP protocol 112, not TCP or UDP
     - The other ``keepalived`` daemons of the service. They use unicast
       unless ``use_keepalived_multicast`` is ``true``.
     - Not applicable
   * - iSCSI
     - 3260
     - iSCSI initiators
     - Not configurable in the specification
   * - iSCSI
     - 5000
     - Addresses in ``trusted_ip_list``. cephadm adds the active Manager's
       address to the list for the dashboard.
     - ``api_port``
   * - NVMe-oF
     - 5500
     - The active Manager (dashboard) and the NVMe-oF CLI
     - ``port``
   * - NVMe-oF
     - 4420
     - NVMe/TCP hosts, for I/O
     - The port of each listener. cephadm reserves and opens only 4420.
   * - NVMe-oF
     - 8009
     - NVMe/TCP hosts, for discovery
     - ``discovery_port``
   * - NVMe-oF
     - 10008
     - Prometheus
     - ``prometheus_port``
   * - SMB
     - 445
     - SMB clients
     - ``custom_ports``, key ``smb``
   * - SMB
     - 9922
     - Prometheus
     - ``custom_ports``, key ``smbmetrics``
   * - SMB
     - 4379, with the ``clustered`` feature
     - The other SMB daemons of the service (CTDB)
     - ``custom_ports``, key ``ctdb``
   * - SMB
     - 54445, with the ``remote-control`` feature
     - Remote-control clients
     - ``custom_ports``, key ``remote-control``

Monitoring and Management Services
==================================

.. list-table::
   :header-rows: 1
   :widths: 15 20 35 30

   * - Service
     - Port
     - Connects from
     - Set with
   * - ``ceph-exporter``
     - 9926
     - Prometheus
     - ``port``
   * - Prometheus
     - 9095
     - Grafana, the Manager (dashboard), and users
     - ``port``
   * - Alertmanager
     - 9093
     - Prometheus and the Manager (dashboard)
     - ``port``
   * - Alertmanager
     - 9094, TCP and UDP
     - The other Alertmanager daemons, for clustering
     - Not configurable
   * - Grafana
     - 3000
     - Browsers, including those that show the dashboard's Grafana panels
     - ``port``
   * - node-exporter
     - 9100
     - Prometheus
     - Not configurable: the ``port`` field does not move it
   * - Loki
     - 3100
     - Promtail or Alloy on each host, and Grafana
     - Not configurable
   * - Promtail and Alloy
     - 9080
     - Nothing: they send logs to Loki. Alloy is available beginning with the
       Umbrella release.
     - Not needed
   * - Pushgateway
     - 9091. Beginning with the Vampire release.
     - RGW hosts, which push metrics to it, and Prometheus
     - ``port``
   * - mgmt-gateway
     - 443, or 80 when the specification sets ``ssl: false``
     - Browsers and API clients
     - ``port``
   * - mgmt-gateway
     - 29443
     - Prometheus, Alertmanager, Grafana, and the Manager
     - Not configurable
   * - oauth2-proxy
     - 4180
     - The mgmt-gateway
     - ``https_address``
   * - snmp-gateway
     - 9464
     - Alertmanager
     - ``port``
   * - Custom container
     - None
     - Depends on the container
     - ``ports``

The Jaeger tracing services use port 6799 for the Jaeger agent, 14250 for the
collector, 16686 for the query service, and 9200 for Elasticsearch. See
:ref:`cephadm-tracing`.

.. _cephadm-ports-firewalld:

How cephadm Configures firewalld
================================

When firewalld is installed, enabled, and running on a host, cephadm changes
its configuration each time it deploys a daemon on that host. The
``--skip-firewalld`` option of ``cephadm bootstrap`` turns this off for the
daemons that ``cephadm bootstrap`` deploys. cephadm makes its changes in the
permanent configuration of firewalld's default zone and then reloads firewalld:

- It enables the firewalld service ``ceph-mon`` for a Monitor, ``ceph`` for an
  OSD, a Manager, or an MDS, and ``nfs`` for an NFS daemon.
- It opens each TCP port that it assigned to the daemon: the default port from
  the tables above, or the port that the service specification sets. For a
  Manager, these are the ports of the Manager modules that
  ``ceph mgr services`` lists, and the service discovery port. For an iSCSI
  gateway, cephadm also opens port 3260.
- ``cephadm bootstrap`` also opens the dashboard's SSL port.

When cephadm removes a daemon, it closes the ports that it opened for that
daemon. It leaves the firewalld services enabled.

firewalld releases earlier than 2.2.0 define the ``ceph`` service as TCP ports
6800-7300, which is narrower than the default range of 6800-7568. On hosts with
such a release, open ports 7301-7568 yourself, or lower
:confval:`ms_bind_port_max`.

cephadm opens only the TCP ports described above. Open the following yourself
where they apply:

- VRRP between the ``keepalived`` daemons of an ingress service.
- UDP port 9094 between the Alertmanager daemons.
- The mgmt-gateway's internal port, 29443.
- The SMB remote-control port, 54445.
- NVMe-oF listener ports other than 4420.
- SSH from the Manager hosts to every host.

On a host where firewalld is not running, cephadm does not change the
firewall. If you use another firewall, open the ports on this page yourself.
