.. _mgr-rgw-module:

RGW Module
============
The rgw module provides a simple interface to deploy RGW multisite.
It helps with bootstrapping and configuring RGW realm, zonegroup and
the different related entities.

Enabling
--------

The *rgw* module is enabled with::

  ceph mgr module enable rgw


RGW Realm Operations
-----------------------

Bootstrapping RGW realm creates a new RGW realm entity, a new zonegroup,
and a new zone. It configures a new system user that can be used for
multisite sync operations. Under the hood this module instructs the
orchestrator to create and deploy the corresponding RGW daemons. The module
supports both passing the arguments through the cmd line or as a spec file:

.. prompt:: bash #

  ceph rgw realm bootstrap [--realm-name] [--zonegroup-name] [--zone-name] [--port] [--placement] [--start-radosgw]

The command supports providing the configuration through a spec file (`-i option`):

.. prompt:: bash #

  ceph rgw realm bootstrap -i myrgw.yaml

Following is an example of RGW multisite spec file:

.. code-block:: yaml

  rgw_realm: myrealm
  rgw_zonegroup: myzonegroup
  rgw_zone: myzone
  placement:
    hosts:
     - ceph-node-1
     - ceph-node-2
  spec:
    rgw_frontend_port: 5500

.. note:: The spec file used by RGW has the same format as the one used by the orchestrator. Thus,
          the user can provide any orchestration supported rgw parameters including advanced
          configuration features such as SSL certificates etc.

Users can also specify custom zone endpoints in the spec (or through the cmd line). In this case, no
cephadm daemons will be launched. Following is an example RGW spec file with zone endpoints:

.. code-block:: yaml

  rgw_realm: myrealm
  rgw_zonegroup: myzonegroup
  rgw_zone: myzone
  zone_endpoints: http://<rgw_host1>:<rgw_port1>, http://<rgw_host2>:<rgw_port2>


Realm Credentials Token
-----------------------

Users can list the available tokens for the created (or already existing) realms.
The token is a base64 string that encapsulates the realm information and its
master zone endpoint authentication data. Following is an example of
the `ceph rgw realm tokens` output:

.. prompt:: bash #

  ceph rgw realm tokens | jq

.. code-block:: json

  [
    {
      "realm": "myrealm1",
      "token": "ewogICAgInJlYWxtX25hbWUiOiAibXlyZWFs....NHlBTFhoIgp9"
    },
    {
      "realm": "myrealm2",
      "token": "ewogICAgInJlYWxtX25hbWUiOiAibXlyZWFs....RUU12ZDB0Igp9"
    }
  ]

User can use the token to pull a realm to create secondary zone on a
different cluster that syncs with the master zone on the primary cluster
by using `ceph rgw zone create` command and providing the corresponding token.

Following is an example of zone spec file:

.. code-block:: yaml

  rgw_zone: my-secondary-zone
  rgw_realm_token: <token>
  placement:
    hosts:
     - ceph-node-1
     - ceph-node-2
  spec:
    rgw_frontend_port: 5500


.. prompt:: bash #

  ceph rgw zone create -i zone-spec.yaml

.. note:: The spec file used by RGW has the same format as the one used by the orchestrator. Thus,
          the user can provide any orchestration supported rgw parameters including advanced
          configuration features such as SSL certificates etc.

Commands
--------
::

  ceph rgw realm bootstrap -i spec.yaml

Create a new realm + zonegroup + zone and deploy rgw daemons via the
orchestrator using the information specified in the YAML file.

::

  ceph rgw realm tokens

List the tokens of all the available realms

::

  ceph rgw zone create -i spec.yaml

Join an existing realm by creating a new secondary zone (using the realm token)

::

  ceph rgw admin [*]

RGW admin command

Automatic usage log trimming
----------------------------

Enable automatic usage log trimming for one or more RGW realms by setting the
``usage_trim_older_than_days`` config option to the amount of days usage logs
older than this should be trimmed.

Optionally you can set the interval in minutes that trim will be performed
with ``usage_trim_interval``, it defaults to every twelve hours (720 minutes).

Optionally you can set the config option ``usage_realms_to_trim`` to a comma
separated list of realms or to an asterisk (``*``, the default for this config
option) for all realms.

The below example will trim logs from start date 1970-01-01 to the current
date minus 30 days for all realms.

::

    ceph config set mgr mgr/rgw/usage_trim_older_than_days 30

The above is equivalent to running the following commands assuming that todays
date is 2022-11-13 and that there are two realms.

::

    radosgw-admin --rgw-realm=realm1 usage trim --start-date=1970-01-01 --end-date=2022-10-14
    radosgw-admin --rgw-realm=realm2 usage trim --start-date=1970-01-01 --end-date=2022-10-14

End date is thus calculated as (today - usage_trim_older_than_days) which is
equal to (2022-11-13 - 30 days) = 2022-10-14.

If you want to only trim the usage log for a realm named ``realm1`` you can
set ``usage_realms_to_trim`` like below.

::

    ceph config set mgr mgr/rgw/usage_realms_to_trim realm1
