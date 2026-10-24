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

.. note::

   This page describes later releases. In Squid 19.2, Tentacle 20.2 and
   Umbrella 21, cephadm runs Docker containers in the foreground with
   ``docker run`` and sets ``Restart=on-failure`` for Docker units as well as
   for Podman units.


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
after a non-zero exit code, a timeout, or termination by a signal other than
``SIGHUP``, ``SIGINT``, ``SIGTERM`` or ``SIGPIPE``) and cleans up both a PID
file and a CID file on each stop.


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
Ceph daemons stay online while the Docker Engine restarts. When it starts
again, the Docker Engine can manage the running containers only if Docker
daemon options such as bridge IP addresses and the graph driver are unchanged;
otherwise, you might need to stop the containers manually. Across Docker
Engine upgrades, Live Restore is supported only for patch releases, which
change only the last number of the version (for example, from 29.9.0 to
29.9.1).

This is the **recommended configuration** for production deployments that use
Docker as the container engine.


Behavior With Live Restore Disabled
=====================================

When Live Restore is disabled (the Docker default), a Docker Engine restart
stops all running containers. Systemd detects that the tracked PID has exited
and, because ``Restart=always`` is set, restarts the service unit after 10
seconds (``RestartSec=10s``).
This brings the container back up, but there is a brief window during which the
Ceph daemon is unavailable.

For most workloads this is acceptable. However, if your deployment requires
continuous daemon availability across engine-level restarts, consider enabling
Live Restore.
