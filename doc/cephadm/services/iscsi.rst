=============
iSCSI Service
=============

.. _cephadm-iscsi:

Deploying iSCSI
===============

To deploy an iSCSI gateway, create a yaml file containing a
service specification for iscsi:

.. code-block:: yaml

    service_type: iscsi
    service_id: iscsi
    placement:
      hosts:
        - host1
        - host2
    spec:
      pool: mypool  # RADOS pool where ceph-iscsi config data is stored.
      trusted_ip_list: "IP_ADDRESS_1,IP_ADDRESS_2"
      api_port: ... # optional
      api_user: ... # optional
      api_password: ... # optional
      api_secure: true/false # optional
      ssl_cert: | # optional
        ...
      ssl_key: | # optional
        ...

For example:

.. code-block:: yaml

    service_type: iscsi
    service_id: iscsi
    placement:
      hosts:
      - [...]
    spec:
      pool: iscsi_pool
      trusted_ip_list: "IP_ADDRESS_1,IP_ADDRESS_2,IP_ADDRESS_3,..."
      api_user: API_USERNAME
      api_password: API_PASSWORD
      ssl_cert: |
        -----BEGIN CERTIFICATE-----
        MIIDtTCCAp2gAwIBAgIYMC4xNzc1NDQxNjEzMzc2MjMyXzxvQ7EcMA0GCSqGSIb3
        DQEBCwUAMG0xCzAJBgNVBAYTAlVTMQ0wCwYDVQQIDARVdGFoMRcwFQYDVQQHDA5T
        [...]
        -----END CERTIFICATE-----
      ssl_key: |
        -----BEGIN PRIVATE KEY-----
        MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5jdYbjtNTAKW4
        /CwQr/7wOiLGzVxChn3mmCIF3DwbL/qvTFTX2d8bDf6LjGwLYloXHscRfxszX/4h
        [...]
        -----END PRIVATE KEY-----

.. py:currentmodule:: ceph.deployment.service_spec

.. autoclass:: IscsiServiceSpec
   :members:


The specification can then be applied using:

.. prompt:: bash #

   ceph orch apply -i iscsi.yaml


See :ref:`orchestrator-cli-placement-spec` for details of the placement specification.

See also: :ref:`orchestrator-cli-service-spec`.

Configuring iSCSI client
========================

The containerized iscsi service can be used from any host by
:ref:`configuring-the-iscsi-initiators`, which will use TCP/IP to send SCSI
commands to the iSCSI target (gateway).

Further Reading
===============

* Ceph iSCSI Overview: :ref:`ceph-iscsi`
