====================
SNMP Gateway Service
====================

SNMP_ is still a widely used protocol, to monitor distributed systems and devices across a variety of hardware
and software platforms. Ceph's SNMP integration focuses on forwarding alerts from its Prometheus Alertmanager
cluster to a gateway daemon. The gateway daemon transforms the alert into an SNMP Notification and sends
it on to a designated SNMP management platform. The gateway daemon is from the ``snmp_notifier``_ project,
which provides SNMP V2c and V3 support (authentication and encryption).

Ceph's SNMP gateway service deploys one instance of the gateway by default. You may increase this
by providing placement information. However, bear in mind that if you enable multiple SNMP gateway daemons,
your SNMP management platform will receive multiple notifications for the same event.

.. _SNMP: https://en.wikipedia.org/wiki/Simple_Network_Management_Protocol
.. _snmp_notifier: https://github.com/maxwo/snmp_notifier

Compatibility
=============
The table below shows the SNMP versions that are supported by the gateway implementation

================ =========== ===============================================
 SNMP Version     Supported  Notes
================ =========== ===============================================
 V1                  ❌      Not supported by ``snmp_notifier``
 V2c                  ✔
 V3 authNoPriv        ✔      uses username/password authentication, without
                             encryption (NoPriv = no privacy)
 V3 authPriv          ✔      uses username/password authentication with
                             encryption to the SNMP management platform
================ =========== ===============================================


Deploying an SNMP Gateway
=========================
Both SNMP V2c and V3 provide credentials support. In the case of V2c, this is just the community string - but for V3
environments you must provide additional authentication information. These credentials are not supported on the command
line when deploying the service. Instead, you must create the service using a credentials file (in YAML format), or
specify the complete service definition in a YAML file.

Command format
--------------

.. prompt:: bash #

   ceph orch apply snmp-gateway <snmp_version:V2c|V3> <destination> [<port:int>] [<engine_id>] [<auth_protocol: MD5|SHA>] [<privacy_protocol:DES|AES>] [<placement>] ...


Usage Notes

- you must supply the ``--snmp-version`` parameter
- the ``--destination`` parameter must be of the format hostname:port (no default)
- you may omit ``--port``. It defaults to 9464
- the ``--engine-id`` is a unique identifier for the device (in hex) and required for SNMP v3 only.
  Suggested value: 8000C53F<fsid> where the fsid is from your cluster, without the '-' symbols
- for SNMP V3, the ``--auth-protocol`` setting defaults to **SHA**
- for SNMP V3, with encryption you must define the ``--privacy-protocol``
- you **must** provide a -i <filename> to pass the secrets/passwords to the orchestrator

Deployment Examples
===================

SNMP V2c
--------
Here's an example for V2c, showing CLI and service based deployments

.. prompt:: bash #

   ceph orch apply snmp-gateway --port 9464 --snmp_version=V2c --destination=192.168.122.73:162 -i ./snmp_creds.yaml

with a credentials file that contains;

.. code-block:: yaml

   ---
   snmp_community: public

Alternatively, you can create a yaml definition for the gateway and apply it from a single file

.. prompt:: bash #

   ceph orch apply -i snmp-gateway.yml

with the file containing the following configuration

.. code-block:: yaml

    service_type: snmp-gateway
    service_name: snmp-gateway
    placement:
      count: 1
    spec:
      credentials:
        snmp_community: public
      port: 9464
      snmp_destination: 192.168.122.73:162
      snmp_version: V2c


SNMP V3 (authNoPriv)
--------------------
Deploying an snmp-gateway service supporting SNMP V3 with authentication only would look like this:

.. prompt:: bash #

   ceph orch apply snmp-gateway --snmp-version=V3 --engine-id=800C53F000000 --destination=192.168.122.1:162 -i ./snmpv3_creds.yml

with a credentials file of the following form:

.. code-block:: yaml

   ---
   snmp_v3_auth_username: myuser
   snmp_v3_auth_password: mypassword

Alternately a ``ceph orch`` service configuration file of the following form:

.. code-block:: yaml

   service_type: snmp-gateway
   service_name: snmp-gateway
   placement:
     count: 1
   spec:
     credentials:
       snmp_v3_auth_password: mypassword
       snmp_v3_auth_username: myuser
     engine_id: 800C53F000000
     port: 9464
     snmp_destination: 192.168.122.1:162
     snmp_version: V3


SNMP V3 (authPriv)
------------------

To define an SNMP V3 gateway service that implements authentication and privacy (encryption), supply two additional values:

.. prompt:: bash #

   ceph orch apply snmp-gateway --snmp-version=V3 --engine-id=800C53F000000 --destination=192.168.122.1:162 --privacy-protocol=AES -i ./snmpv3_creds.yml

with a credentials file of the following form:

.. code-block:: yaml

   ---
   snmp_v3_auth_username: myuser
   snmp_v3_auth_password: mypassword
   snmp_v3_priv_password: mysecret


.. note::

   The credentials are stored on the host, restricted to the ``root`` user and passed to the ``snmp_notifier`` daemon as
   an environment file (``--env-file``), to limit exposure.


AlertManager Integration
========================
When an SNMP gateway service is deployed or updated, the Prometheus Alertmanager configuration is automatically updated to forward any
alert that has an OID_ label to the SNMP gateway daemon for processing.

.. _OID: https://en.wikipedia.org/wiki/Object_identifier

Implementing the MIB
======================
To make sense of SNMP notifications and traps, you'll need to apply the MIB to your SNMP management platform. The MIB (``CEPH-MIB.txt``) can
downloaded from the main Ceph GitHub repository_

.. _repository: https://github.com/ceph/ceph/tree/master/monitoring/snmp
