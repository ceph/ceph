
===========================
Compatibility and Stability
===========================

.. _cephadm-stability:

Stability
---------

Cephadm is relatively stable but new functionality is still being
added and bugs are occasionally discovered. If issues are found, please
open a tracker issue under the Orchestrator component (https://tracker.ceph.com/projects/orchestrator/issues)

Cephadm support remains under development for the following features:

- stretch mode integration
- monitoring stack (moving towards Prometheus service discovery and providing TLS)
- mgmt-gateway and oauth2 services
- RGW multisite deployment support (requires lots of manual steps currently)
- cephadm agent
- multi-arch clusters (recommended to turn off mgr/cephadm/use_repo_digest for these currently)
- SMB deployment

If a cephadm command fails or a service stops running properly, see
:ref:`cephadm-pause` for instructions on how to pause the Ceph cluster's
background activity and how to disable cephadm.
