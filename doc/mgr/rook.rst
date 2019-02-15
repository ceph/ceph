
=============================
Rook orchestrator integration
=============================

Rook (https://rook.io/) is an orchestration tool that can run Ceph inside
a Kubernetes cluster.

The ``rook`` module provides integration between Ceph's orchestrator framework
(used by modules such as ``dashboard`` to control cluster services) and
Rook.

Orchestrator modules only provide services to other modules, which in turn
provide user interfaces.  To try out the rook module, you might like
to use the :ref:`Orchestrator CLI <orchestrator-cli-module>` module.

Requirements
------------

- Running ceph-mon and ceph-mgr services that were set up with Rook in
  Kubernetes.
- Rook 0.9 or newer.

Configuration
-------------

Because a Rook cluster's ceph-mgr daemon is running as a Kubernetes pod, 
the ``rook`` module can connect to the Kubernetes API without any explicit
configuration.

Development
-----------

If you are a developer, please see :ref:`kubernetes-dev` for instructions
on setting up a development environment to work with this.


