.. _mgr-rgw-module:

RGW Module
============
The ``rgw`` module provides a simple interface to deploy RGW :ref:`multisite`.
It helps with bootstrapping and configuring RGW realm, zonegroup and
the different related entities.

Enabling
--------

To enable the ``rgw`` module, run the following command:

.. prompt:: bash #

   ceph mgr module enable rgw


RGW Realm Operations
--------------------

Bootstrapping RGW realm creates a new RGW realm entity, a new zonegroup,
and a new zone. It configures a new system user that can be used for
multisite sync operations. Under the hood this module instructs the
orchestrator to create and deploy the corresponding RGW daemons. The module
supports passing the arguments through the command line or as a spec file:

.. prompt:: bash #

   ceph rgw realm bootstrap [--realm-name] [--zonegroup-name] [--zone-name] [--port] [--placement] [--start-radosgw]

The command supports configuration through a spec file (``-i`` option):

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

.. note:: RGW multisite spec files follow the same format as cephadm
          orchestrator spec files. Thus the user can specify any supported RGW
          parameters including advanced configuration features such as SSL
          certificates etc.

Users can also specify custom zone endpoints in the spec (or through the command line). In this case, no
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
the ``ceph rgw realm tokens`` output:

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
by using ``ceph rgw zone create`` command and providing the corresponding token.

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
          the user can provide any orchestration supported RGW parameters including advanced
          configuration features such as SSL certificates etc.

Commands
--------

.. prompt:: bash #

   ceph rgw realm bootstrap -i spec.yaml

Create a new realm + zonegroup + zone and deploy RGW daemons via the
orchestrator using the information specified in the YAML file.

.. prompt:: bash #

   ceph rgw realm tokens

List the tokens of all the available realms

.. prompt:: bash #

   ceph rgw zone create -i spec.yaml

Join an existing realm by creating a new secondary zone (using the realm token)

.. prompt:: bash #

   ceph rgw admin [*]

Upgrading Root CA Certificates
------------------------------

#. Make sure that the RGW service is running.
#. Make sure that the RGW service is up.
#. Make sure that the RGW service has been upgraded to the latest release.
#. From the primary cluster on the Manager node, run the following command:

   .. prompt:: bash #

      ceph orch cert-store get cert cephadm_root_ca_cert

#. On the nodes where the RGW service is running, store the certificate on the
   following path::

      /etc/pki/ca-trust/source/anchors/<cert_name>.crt

#. Verify the certificate by running the following command:

   .. prompt:: bash #

      openssl x509 -in <cert_name>.crt -noout -text

#. Perform the above steps on the Manager node and on the RGW nodes of all
   secondary clusters.

#. After the certificates have been validated on all clusters, run the
   following command on all clusters that generate certificates: 

   .. prompt:: bash #

      update-ca-trust

#. From the primary cluster, ensure that the ``curl`` command can be run by the
   user:

   .. prompt:: bash [user@primary-node]$ 

      curl https://<host_ip>:443
