.. _dashboard-feature-toggles:

Feature Toggles
^^^^^^^^^^^^^^^

This plug-in turns individual Ceph Dashboard features on or off. When a
feature becomes disabled:

- Its front-end elements (web pages, menu entries, charts, etc.) will become
  hidden.
- Its associated REST API endpoints will reject any further requests (404, Not
  Found Error).

The main purpose of this plug-in is to allow ad hoc customizations of the
workflows exposed by the dashboard. Additionally, it could allow for dynamically
enabling experimental features with minimal configuration burden and no service
impact.

The list of features that can be enabled/disabled is:

- **Block (RBD)**:

  - Image Management: ``rbd``
  - Mirroring: ``mirroring``
  - iSCSI: ``iscsi``

- **File System (CephFS)**: ``cephfs``
- **Objects (RGW)**: ``rgw`` (including daemon, user and bucket management).
- **NFS**: ``nfs`` exports.
- **Landing page** (Squid 19.2 and Tentacle 20.2 only): ``dashboard``.

By default all features come enabled.

To retrieve a list of features and their current statuses:

.. prompt:: bash #

   ceph dashboard feature status

::

    Feature 'rbd': enabled
    Feature 'mirroring': enabled
    Feature 'iscsi': enabled
    Feature 'cephfs': enabled
    Feature 'rgw': enabled
    Feature 'nfs': enabled

In Squid 19.2 and Tentacle 20.2 the list ends with ``Feature 'dashboard'``.

To enable or disable the status of a single or multiple features:

.. prompt:: bash #

   ceph dashboard feature disable iscsi mirroring

::

   Feature 'iscsi': disabled
   Feature 'mirroring': disabled

After a feature status has changed, the REST API endpoints can take up to 10
seconds to respond to the change, because the Ceph Manager caches the status
of each feature for 10 seconds. The Dashboard checks the feature status every
30 seconds, so the front-end UI elements can take up to 40 seconds to reflect
the change.
