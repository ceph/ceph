.. _cli-version-tracker:

=========================
Version Tracking for Ceph
=========================

Cephadm tracks the history of cluster versions and info about the
current state of the Ceph cluster at time of upgrade. There are two CLI
commands to manage this information. Version information from 
before the introduction of this feature is not available.

Viewing Version History
=======================

.. prompt:: bash #

    ceph cephadm get-cluster-version-history [--show-config]

This command displays stored version history stored in 
chronological order. If ``--show-config`` is passed the
recorded config dump is also displayed.

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null
        },
        "2026-06-23 00:51:29": {
            "version": "quay.io/ceph/ceph:v19.2.3",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v19.2.3",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }
     
Option ``--show-config``:

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null,
            "config_dump": [
                {
                    "section": "global",
                    "name": "container_image",
                    "value": "quay.io/ceph/ceph:v18.2.8",
                    "level": "basic",
                    "can_update_at_runtime": false,
                    "mask": ""
                },
                {
                    "section": "global",
                    "name": "public_network",
                    "value": "0.0.0.0/0",
                    "level": "advanced",
                    "can_update_at_runtime": false,
                    "mask": ""
                },
                {
                    "section": "mon",
                    "name": "auth_allow_insecure_global_id_reclaim",
                    "value": "false",
                    "level": "advanced",
                    "can_update_at_runtime": true,
                    "mask": ""
                },
                {
                    "section": "mgr",
                    "name": "mgr/cephadm/container_init",
                    "value": "True",
                    "level": "advanced",
                    "can_update_at_runtime": false,
                    "mask": ""
                },
                {
                    "section": "mgr",
                    "name": "mgr/cephadm/migration_current",
                    "value": "6",
                    "level": "advanced",
                    "can_update_at_runtime": false,
                    "mask": ""
                },
                {
                    "section": "mgr",
                    "name": "mgr/dashboard/ssl_server_port",
                    "value": "2000",
                    "level": "advanced",
                    "can_update_at_runtime": false,
                    "mask": ""
                },
                {
                    "section": "mgr",
                    "name": "mgr/orchestrator/orchestrator",
                    "value": "cephadm",
                    "level": "advanced",
                    "can_update_at_runtime": true,
                    "mask": ""
                },
                {
                    "section": "osd",
                    "name": "osd_memory_target_autotune",
                    "value": "true",
                    "level": "advanced",
                    "can_update_at_runtime": true,
                    "mask": ""
                }
            ]
        }
    }


Removing Version History (Not Recommended)
==========================================

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history [--all] [--before <datetime>] [--after <datetime>]

This command will allow users to delete version history and requires
at least one of the three provided options to be passed. If ``--all`` is
passed all version history is deleted, this option is incompatible
with ``--before`` and ``--after`` and returns an error if either are passed
with it. Supply ``--before`` to specify deletion of version history before
the <datetime> specified. Supply ``--after`` to specify deletion of version
history after the <datetime> specified. The parameters ``--after`` and
``--before`` can be used together to specify a range for version history
deletion. The format of <datetime> should be "YYYY-MM-DD HH:MM:SS".

Option ``--all``:

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null
        },
        "2026-06-23 00:51:29": {
            "version": "quay.io/ceph/ceph:v19.2.3",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v19.2.3",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --all

::

    {
        No Cluster Version History Stored
    }

Option ``--before``:

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null,
        },
        "2026-06-23 00:51:29": {
            "version": "quay.io/ceph/ceph:v19.2.3",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v19.2.3",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --before "2026-06-23 00:00:00"

.. code-block:: json

    {
        "2026-06-23 00:51:29": {
            "version": "quay.io/ceph/ceph:v19.2.3",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v19.2.3",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }

Option ``--after``:

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null
        },
        "2026-06-23 00:51:29": {
            "version": "quay.io/ceph/ceph:v19.2.3",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v19.2.3",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --after "2026-06-23 00:00:00"

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null
        }
    }

Options ``--before`` and ``--after``:

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null
        },
        "2026-06-23 00:51:29": {
            "version": "quay.io/ceph/ceph:v19.2.3",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v19.2.3",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        },
        "2026-06-24 00:10:01": {
            "version": "quay.io/ceph/ceph:v20.2.1",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v20.2.1",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }

.. prompt:: bash #

    ceph cephadm remove-cluster-version-history --after "2026-06-22 23:00:00" --before "2026-06-24 00:00:00"

.. code-block:: json

    {
        "2026-06-22 22:33:43": {
            "version": "ceph version 18.2.8 (asdfac5eee07c13fa5048sd230242b866df) reef (stable)",
            "upgrade_type": "bootstrap",
            "status": "complete",
            "command_options": null
        },
        "2026-06-24 00:10:01": {
            "version": "quay.io/ceph/ceph:v20.2.1",
            "upgrade_type": "full",
            "status": "complete",
            "command_options": {
                "image": "quay.io/ceph/ceph:v20.2.1",
                "version": null,
                "daemon_types": null,
                "hosts": null,
                "services": null,
                "limit": null
            }
        }
    }
    




