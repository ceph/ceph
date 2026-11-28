.. _cephadm-config-options:

=============================
Cephadm Configuration Options
=============================

The options on this page control the ``cephadm`` Manager module. They are
stored in the Ceph configuration database and are set with
``ceph config set mgr mgr/cephadm/<option> <value>``. For example, to see and
then change how long cephadm caches the device inventory of each host, run the
following commands:

.. prompt:: bash #

   ceph config get mgr mgr/cephadm/device_cache_timeout
   ceph config set mgr mgr/cephadm/device_cache_timeout 900

To restore the default value of an option, remove the setting:

.. prompt:: bash #

   ceph config rm mgr mgr/cephadm/device_cache_timeout

.. mgr_module:: cephadm

Containers and Registries
=========================

These options set the Ceph container image name, how cephadm qualifies and
pins image names, and how cephadm runs containers.

.. confval:: container_image_base
.. confval:: default_registry
.. confval:: use_repo_digest
.. confval:: registry_insecure
.. confval:: container_init
.. confval:: cgroups_split
.. confval:: allow_ptrace

Default Container Images
========================

These options set the image of each service that does not run the Ceph
container image. See :ref:`cephadm_default_images`.

.. confval:: container_image_prometheus
.. confval:: container_image_loki
.. confval:: container_image_promtail
.. confval:: container_image_alloy
.. confval:: container_image_node_exporter
.. confval:: container_image_alertmanager
.. confval:: container_image_grafana
.. confval:: container_image_haproxy
.. confval:: container_image_keepalived
.. confval:: container_image_nvmeof
.. confval:: container_image_snmp_gateway
.. confval:: container_image_elasticsearch
.. confval:: container_image_jaeger_collector
.. confval:: container_image_jaeger_agent
.. confval:: container_image_jaeger_query
.. confval:: container_image_samba
.. confval:: container_image_samba_metrics
.. confval:: container_image_nginx
.. confval:: container_image_pushgateway
.. confval:: container_image_oauth2_proxy

SSH and Remote Execution
========================

These options control how cephadm connects to hosts over SSH and runs
commands on them. See :ref:`cephadm-ssh-configuration` and
:ref:`cephadm-sudo-hardening`.

.. confval:: ssh_config_file
.. confval:: mode
.. confval:: ssh_keepalive_interval
.. confval:: ssh_keepalive_count_max
.. confval:: default_cephadm_command_timeout
.. confval:: sudo_hardening

Host Management
===============

These options set how often cephadm refreshes what it knows about each host,
which routes it reports, where it manages ``ceph.conf``, and how many daemons
of one service it places on a host. See :ref:`etc_ceph_conf_distribution`.

.. confval:: host_check_interval
.. confval:: facts_cache_timeout
.. confval:: daemon_cache_timeout
.. confval:: allow_lo_routes
.. confval:: allow_bgp_routes
.. confval:: manage_etc_ceph_ceph_conf
.. confval:: manage_etc_ceph_ceph_conf_hosts
.. confval:: max_count_per_host

OSDs and Devices
================

These options control the device inventory, OSD memory autotuning, and OSD
removal. See :ref:`osd_autotune` and :ref:`cephadm-osd-removal`.

.. confval:: device_cache_timeout
.. confval:: device_enhanced_scan
.. confval:: inventory_list_all
.. confval:: autotune_memory_target_ratio
.. confval:: autotune_interval
.. confval:: max_osd_draining_count

Health Checks
=============

These options control the health warnings that cephadm raises for stray
hosts, stray daemons, and failed host checks, and the cluster configuration
checks. See "Health Checks" in :doc:`operations`.

.. confval:: stray_daemon_check_interval
.. confval:: warn_on_stray_hosts
.. confval:: warn_on_stray_daemons
.. confval:: warn_on_failed_host_check
.. confval:: config_checks_enabled

Upgrades
========

These options control the checks that cephadm runs before an upgrade and how
many daemons it upgrades at a time. See :doc:`upgrade`.

.. confval:: upgrade_cpu_isa_check
.. confval:: max_parallel_osd_upgrades
.. confval:: max_gateway_upgrades_per_pass
.. confval:: pg_autoscale_during_upgrade

Monitoring and Dashboard
========================

These options set the alert and dashboard files, the service discovery port,
and TLS for the monitoring stack that cephadm deploys, and whether cephadm
configures the Dashboard to use the services that it deploys. See
:ref:`mgr-cephadm-monitoring`.

.. confval:: prometheus_alerts_path
.. confval:: grafana_dashboards_path
.. confval:: service_discovery_port
.. confval:: secure_monitoring_stack
.. confval:: config_dashboard

Hardware Monitoring
===================

These options control node-proxy, which reads hardware status from the
out-of-band management interface of each host. See :ref:`hardware-monitoring`.

.. confval:: hw_monitoring
.. confval:: hw_monitoring_vendor
.. confval:: oob_default_addr

Cephadm Agent
=============

These options control the cephadm agent, an optional daemon on each host that
gathers host metadata and sends it to the active Manager.

.. confval:: use_agent
.. confval:: agent_refresh_rate
.. confval:: agent_starting_port
.. confval:: agent_down_multiplier

Certificates
============

These options control how cephadm checks, renews, and signs the TLS
certificates that it manages. See :ref:`orchestrator-cli-cert-management`.

.. confval:: certificate_automated_rotation_enabled
.. confval:: certificate_check_period
.. confval:: certificate_duration_days
.. confval:: certificate_renewal_threshold_days

Logging and Debugging
=====================

These options control what cephadm logs, and where, for the module itself,
for the ``cephadm`` binary on each host, for ``ceph-volume``, and for SSH
connections. See :ref:`cephadm-logs` and :ref:`cephadm-asyncssh-log-verbosity`.

.. confval:: log_to_cluster
.. confval:: log_refresh_metadata
.. confval:: log_deploy_configuration
.. confval:: cephadm_log_destination
.. confval:: cephadm_binary_logging_level
.. confval:: ceph_volume_log_level
.. confval:: asyncssh_log_level
.. confval:: asyncssh_debug_level
