.. _cephadm-docker-live-restore:

=======================================
Cephadm and Docker Engine: Live Restore
=======================================

When Docker is used as the container engine, cephadm configures each Ceph
daemon's systemd service unit differently from how it configures Podman-based
units. This section describes those differences and explains how Docker's
optional `Live Restore
<https://docs.docker.com/engine/daemon/live-restore/>`_ feature is facilitated
with cephadm-managed daemons.


How Cephadm Runs Containers with Docker
========================================

Cephadm starts Docker-managed containers in **detached mode** (``docker run
-d``). The container therefore runs in the background rather than being tied to
the foreground process that systemd spawns. To allow systemd to keep track of
the container, cephadm inspects the container immediately after it starts,
retrieves its PID via ``docker inspect``, and writes the parent PID to a PID
file. The systemd unit is configured as ``Type=forking`` and ``PIDFile`` points
to that file.

The systemd unit also sets ``Restart=always``, which means systemd will restart
the service whenever the tracked process exits for any reason.

This differs from Podman, where cephadm uses ``Restart=on-failure`` (restarting
only on non-zero exit) and cleans up both a PID file and a CID file on each
stop.


Behavior With Live Restore Enabled
====================================

When `Live Restore <https://docs.docker.com/engine/daemon/live-restore/>`_ is
enabled in the Docker daemon configuration, containers continue running if the
Docker Engine process is down or restarted for example during a system update via 
the command:
.. prompt:: bash #
     systemctl restart docker.service

The PID tracked by systemd remains valid throughout the daemon restart, so 
systemd does not trigger a service restart.
Ceph daemons stay online across Docker Engine upgrades or unplanned daemon
restarts with no disruption.

This is the **recommended configuration** for production deployments that use
Docker as the container engine.


Behavior With Live Restore Disabled
=====================================

When Live Restore is disabled (the Docker default), a Docker Engine restart
stops all running containers. Systemd detects that the tracked PID has exited
and, because ``Restart=always`` is set, immediately restarts the service unit.
This brings the container back up, but there is a brief window during which the
Ceph daemon is unavailable.

For most workloads this is acceptable. However, if your deployment requires
continuous daemon availability across engine-level restarts, consider enabling
Live Restore.
