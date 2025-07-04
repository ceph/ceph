.. _orchestrator-cli-agent-management:

=================
Cephadm Agent
=================

Introduction
============

The `cephadm` Agent is a lightweight daemon designed to run on each node in a Ceph cluster. Its role is to periodically gather and send host
metadata to the cephadm orchestrator (running in the Ceph Manager). This decentralized approach reduces the burden on the central Ceph manager,
especially in large-scale deployments. Hence, using the agent is **recommended for large clusters** to improve scalability and reduce metadata
collection latency.

Agent Operation Overview
=========================

When enabled, each `cephadm` agent performs the following:

- Gathers host-specific metadata such as services, storage, networking, and services state.
- Compresses and optimizes metadata to reduce transmission size and network overhead.
- Sends metadata back to the manager at intervals based on configuration or auto-adjusted parameters.

This design supports both scalability and performance in dense distributed environments.

Configuration
=============

Several Ceph configuration options are available to control agent behavior and performance:

- ``mgr/cephadm/use_agent`` (default: ``False``):

  Enables the use of `cephadm` agents on each host to send metadata to the manager. This must be explicitly
  set to ``True`` to activate agent functionality.

- ``mgr/cephadm/agent_refresh_rate`` (default: ``-1``):

  Controls how frequently (in seconds) each agent sends metadata. If set to ``-1``, the system automatically
  adjusts the refresh rate based on the cluster size to balance responsiveness and overhead.

- ``mgr/cephadm/agent_avg_concurrency`` (default: ``-1``):

  Sets the target average number of agents reporting per second. Used to compute a jitter window to avoid request
  bursts. Set to ``-1`` to allow automatic tuning based on cluster scale.

- ``mgr/cephadm/agent_initial_startup_delay_max`` (default: ``-1``):

  Specifies the maximum random startup delay (in seconds) before an agent begins sending metadata. A value
  of ``-1`` enables automatic behavior. Set to ``0`` to disable the startup delay.

- ``mgr/cephadm/agent_jitter_seconds`` (default: ``-1``):

  Adds a random delay (in seconds) before each metadata transmission to prevent synchronized bursts
  across hosts. ``-1`` enables automatic jitter; ``0`` disables jitter.

- ``mgr/cephadm/agent_starting_port`` (default: ``4721``):

  Defines the first TCP port an agent attempts to bind to for communication. If that port is in use, the agent
  will try the next 1,000 subsequent ports until successful.

- ``mgr/cephadm/agent_metadata_compresion_enabled`` (default: ``True``):

  When enabled, agents compress metadata before transmission to reduce payload size and network usage. This is particularly
  helpful for clusters with big number of nodes or fewer dense nodes.

- ``mgr/cephadm/agent_metadata_payload_optimization_enabled`` (default: ``True``):

  Optimizes metadata payloads by excluding unchanged sections (e.g., volume lists, network interfaces). This minimizes data sent
  across the network and reduces processing overhead on the manager side.

- ``mgr/cephadm/agent_down_multiplier`` (default: ``3.0``):

  Determines how long a manager will wait before marking an agent as "down." This is calculated as `agent_refresh_rate * agent_down_multiplier`.


.. note::

   When manually configuring these parameters, it is important to choose consistent values that work well together:

   - If you set ``agent_refresh_rate`` to a fixed value (e.g., ``10``), you should **also** set a
     fixed ``agent_avg_concurrency`` and ``agent_jitter_seconds``. This ensures the system doesn't mix auto-tuned
     jitter or concurrency with a fixed refresh rate, which could lead to uneven metadata reporting patterns.

   - If you leave ``agent_refresh_rate`` as ``-1`` (auto), it is recommended to also leave ``agent_avg_concurrency``
     and ``agent_jitter_seconds`` set to ``-1`` so that the manager can tune them together as a group.

   - ``agent_initial_startup_delay_max`` can typically remain on ``-1`` (auto) unless you are testing, debugging, or
     need agents to begin reporting immediately, in which case you may set it to ``0``.

   - In general, for **large clusters**, it is safest to leave all four parameters set to ``-1`` to let Cephadm
     auto-tune them based on the size and behavior of the cluster.

   Mixing auto and manual values is allowed but **discouraged**, as it may lead to suboptimal behavior unless you
   fully understand the implications of each setting.


Agent Health Monitoring
=======================

Cephadm monitors the activity of each registered agent. If a host agent fails to report within the expected timeframe
(based on `refresh_rate` and `down_multiplier`), the host will be flagged as stale or down. This ensures administrators
are alerted when a node is not communicating and can investigate connectivity or daemon health issues proactively.


Managing Agent Behavior
=======================

Reloading Agent Configuration
-----------------------------

To reload the cephadm agent configuration on the manager:

.. prompt:: bash #

   ceph orch reconfig agent

This command reconfigure all the running agents so they can reload their configuration such as port binding and refresh rates, without needing a manager restart.

Get Agent Configuration
-----------------------

Several agent parameters support auto-tuning by using a value of ``-1``. To view the effective values currently in
use (including those automatically computed), run the following command:

.. prompt:: bash #

   ceph orch agent show-config


Listing Agent Status
--------------------

To list all agents and their statuses:

.. prompt:: bash #

   ceph orch ps --service-name agent

This command provides the current status of agents, including last report time and daemons status.


Agent Use Case Scenarios
========================

- **Small Clusters (<10 hosts):**
  - Using agents is optional. The manager can gather metadata efficiently in small environments.

- **Large Clusters (100+ hosts):**
  - Strongly recommended to use agents to avoid manager overload and to enable scalable metadata collection.

- **Geographically Distributed Clusters:**
  - Agents reduce cross-site metadata pull latency by pushing data asynchronously.

Summary
=======

The `cephadm` agent provides a scalable, efficient mechanism for metadata collection and monitoring across large Ceph clusters.
With flexible configuration, built-in optimization, and proactive health tracking, it is a critical component for managing large
and complex Ceph deployments.
