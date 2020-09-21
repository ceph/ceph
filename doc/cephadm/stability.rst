.. _cephadm-stability:

Stability
=========

Cephadm is a new feature in the Octopus release and has seen limited
use in production and at scale.  We would like users to try cephadm,
especially for new clusters, but please be aware that some
functionality is still rough around the edges.  We expect fairly
frequent updates and improvements over the first several bug fix
releases of Octopus.

Cephadm management of the following components are currently well-supported:

- Monitors
- Managers
- OSDs
- CephFS file systems
- rbd-mirror

The following components are working with cephadm, but the
documentation is not as complete as we would like, and there may be some
changes in the near future:

- RGW
- dmcrypt OSDs

Cephadm support for the following features is still under development:

- NFS
- iSCSI

If you run into problems, you can always pause cephadm with::

  ceph orch pause

Or turn cephadm off completely with::

  ceph orch set backend ''
  ceph mgr module disable cephadm

