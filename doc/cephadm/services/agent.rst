.. _cephadm-agent:

=============
Cephadm Agent
=============

By default, the cephadm manager module gathers information about each host
(running daemons, devices, networks and host facts) by connecting to the host
over SSH. In agent mode, cephadm instead deploys an ``agent`` daemon on every
host. Each agent gathers this information and sends it to the active manager
over HTTPS. The manager sends requests back to the agent through a listener
that the agent runs on the host.

.. note::

   Agent mode is still under development. See :ref:`cephadm-stability`.

Enabling and disabling the agent
================================

The agent service is managed by the ``mgr/cephadm/use_agent`` setting. Use
this setting, and only this setting, to deploy or remove the agents.

To deploy an agent on every host, run:

.. prompt:: bash #

   ceph config set mgr mgr/cephadm/use_agent true

To remove the agents and return to SSH-based host refreshes, run:

.. prompt:: bash #

   ceph config set mgr mgr/cephadm/use_agent false

When the setting changes, cephadm creates or removes the ``agent`` service on
its next pass. Turning ``use_agent`` back on while the agents are still being
removed cancels the removal.

Cephadm always uses the same agent service specification: an agent on every
host, with a listener certificate signed by the cephadm root CA. There are no
settings in the specification that can be changed:

- ``ceph orch apply`` with an ``agent`` specification is refused while
  ``use_agent`` is ``false``. While ``use_agent`` is ``true``, the applied
  specification is replaced by the default one, and the command prints a
  warning if they differ. This can be used to restore the default
  specification.
- ``ceph orch rm agent`` is refused while ``use_agent`` is ``true``.

The usual commands to inspect and manage daemons still work for the agent
service, for example ``ceph orch ls agent``, ``ceph orch ps --daemon_type
agent`` and ``ceph orch daemon redeploy agent.<host>``.

Settings
========

The following manager settings control the agents:

``mgr/cephadm/agent_refresh_rate``
   How often, in seconds, each agent gathers and sends host information.
   Default: 20.

``mgr/cephadm/agent_starting_port``
   The first TCP port on which the agent tries to start its listener. If the
   port is in use, the agent tries the following ports. Default: 4721.

``mgr/cephadm/agent_down_multiplier``
   An agent that has not reported for this many refresh periods is considered
   down, and the ``CEPHADM_AGENT_DOWN`` health warning is raised. Default: 3.0.

Switching to agent mode
=======================

In agent mode, cephadm applies service specifications only when it has
up-to-date information from every reachable host. After ``use_agent`` is
turned on, cephadm therefore first deploys the agents and waits for each of
them to report. Until then, only the agent service is applied.

While cephadm is waiting, it logs which hosts it is waiting for and why, and
records the same message as an event on the agent service. To see it, run:

.. prompt:: bash #

   ceph orch ls agent --format yaml

The message groups the hosts as follows:

- hosts without an agent daemon: the agent has not been deployed there yet;
- hosts whose agent has not reported yet;
- hosts whose agent has not acknowledged the latest request from the manager.

The message also says when the wait cannot end on its own, for example because
the agent service is unmanaged. In that case, run ``ceph orch set-managed
agent``.

Troubleshooting
===============

If an agent service specification was applied by hand in an earlier release,
for example without ``ssl: true``, the agents may have been deployed without
listener credentials. Re-apply the default specification and redeploy the
agents:

.. prompt:: bash #

   ceph orch ls agent --export > agent.yaml
   ceph orch apply -i agent.yaml
   ceph orch redeploy agent

``ceph orch apply`` replaces the exported specification with the default one.
