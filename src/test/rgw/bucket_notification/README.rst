==========================
 Bucket Notification Tests
==========================

You will need to use the sample configuration file named ``bntests.conf.SAMPLE``
that has been provided at ``/path/to/ceph/src/test/rgw/bucket_notification/``. You can also copy this file to the directory where you are
running the tests and modify it if needed. This file can be used to run the bucket notification tests on a Ceph cluster started
with the `vstart.sh` script.
For the tests covering Kafka and RabbitMQ security, the RGW will need to accept use/password without TLS connection between the client and the RGW.
So, the cluster will have to be started with the following ``rgw_allow_notification_secrets_in_cleartext`` parameter set to ``true``.

The test suite can be run against a multisite setup, in the configuration file we will have to decide which RGW and which cluster will be used for the test,
and using which version (v1 or v2) of topics/notifications we should use. By default, a new cluster would use v2, which means that if a realm was never defined, v1 cannot be used.
For example, if the ``test-rgw-multisite.sh`` script is used to setup multisite, and we want to test v1 against the first RGW in the first cluster, 
we would need the following configuration file::

				[DEFAULT]
				port = 8101
				host = localhost
				zonegroup = zg1
				cluster = c1
				version = v1

				[s3 main]
				access_key = 0987654321
				secret_key = crayon

Add boto3 extension to the standard client: https://github.com/ceph/ceph/tree/main/examples/rgw/boto3#introduction.

===========
Kafka Tests
===========

You also need to install Kafka which can be downloaded from: https://kafka.apache.org/downloads

Then edit the Kafka server properties file (``/path/to/kafka/config/server.properties``)
to have the following line::

        listeners=PLAINTEXT://localhost:9092

After following the above steps, start the Zookeeper and Kafka services.
For starting Zookeeper service run::

        bin/zookeeper-server-start.sh config/zookeeper.properties

and then start the Kafka service::

        bin/kafka-server-start.sh config/server.properties

If you want to run Zookeeper and Kafka services in the background add ``-daemon`` at the end of the command like::

        bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

and::

        bin/kafka-server-start.sh -daemon config/server.properties

After running `vstart.sh`, Zookeeper, and Kafka services you're ready to run the Kafka tests::

        BNTESTS_CONF=bntests.conf python -m pytest -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -m 'kafka_test'

------------------------------
Kafka 4.x setup (KRaft mode)
------------------------------

Kafka 4.0 removed Zookeeper. The broker uses its own KRaft consensus,
so there is no separate Zookeeper process to start.

First-time setup (per Kafka installation), format the storage::

        KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
        bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/server.properties --standalone

The ``--standalone`` flag is required for single-node test setups.

Then start the broker::

        bin/kafka-server-start.sh config/server.properties

For security tests, the same ``server.properties`` edits in the section
below apply. The KRaft controller listener (e.g. ``CONTROLLER://:9097``)
must remain in the ``listeners`` line on a port outside the 9092-9096
range, since those are reserved by the security listeners. Do not set
``controller.quorum.voters``; the ``--standalone`` flag passed to
``kafka-storage.sh format`` writes the voter set into the bootstrap
metadata automatically, and Kafka 4.1.1+ rejects the combination of
``--standalone`` with an explicit ``controller.quorum.voters``.

--------------------
Kafka Security Tests
--------------------

1. First generate SSL certificates::

        cd /path/to/kafka/
        KAFKA_CERT_HOSTNAME=192.168.1.100 KAFKA_CERT_IP=192.168.1.100 bash /path/to/ceph/src/test/rgw/bucket_notification/kafka-security.sh
   
   Replace ``192.168.1.100`` with your actual Kafka broker's IP address or hostname.

2. Configure Kafka::

   Then edit ``/path/to/kafka/config/server.properties`` with complete configuration::

   **Important:** If you face any initialization failures, replace ``localhost`` in both ``listeners`` and ``advertised.listeners`` with your Kafka broker's actual hostname or IP address.
   For example, if your Kafka broker runs on host ``kafka-server.example.com`` or IP ``192.168.1.100``, use::

   listeners=PLAINTEXT://192.168.1.100:9092,SSL://192.168.1.100:9093,SASL_SSL://192.168.1.100:9094,SASL_PLAINTEXT://192.168.1.100:9095,MTLS://192.168.1.100:9096
   advertised.listeners=PLAINTEXT://192.168.1.100:9092,SSL://192.168.1.100:9093,SASL_SSL://192.168.1.100:9094,SASL_PLAINTEXT://192.168.1.100:9095,MTLS://192.168.1.100:9096

   If both ``listeners`` and ``advertised.listeners`` do not match, the broker cannot connect to itself, causing initialization failures.

        # All listeners (including MTLS on port 9096)
        listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094,SASL_PLAINTEXT://localhost:9095,MTLS://localhost:9096
        advertised.listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094,SASL_PLAINTEXT://localhost:9095,MTLS://localhost:9096
        listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,MTLS:SSL

        # SSL configuration matching the kafka-security.sh script
        ssl.keystore.location=./server.keystore.jks
        ssl.keystore.password=mypassword
        ssl.key.password=mypassword
        ssl.truststore.location=./server.truststore.jks
        ssl.truststore.password=mypassword

        # Allow optional client certificates on the main SSL listener (port 9093).
        # Use "requested" so that plain SSL tests (without client certs) still work,
        # while mTLS tests use a separate listener with ssl.client.auth=required.
        ssl.client.auth=requested

        # mTLS listener: requires client certificate authentication
        listener.name.mtls.ssl.client.auth=required
        listener.name.mtls.ssl.keystore.location=./server.keystore.jks
        listener.name.mtls.ssl.keystore.password=mypassword
        listener.name.mtls.ssl.key.password=mypassword
        listener.name.mtls.ssl.truststore.location=./server.truststore.jks
        listener.name.mtls.ssl.truststore.password=mypassword

        # SASL mechanisms
        sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512,GSSAPI
        sasl.kerberos.service.name=kafka
        sasl.mechanism.inter.broker.protocol=PLAIN
        inter.broker.listener.name=PLAINTEXT

        # SASL over SSL with PLAIN mechanism
        listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
        username="admin" \
        password="admin-secret" \
        user_alice="alice-secret";

        # SASL over SSL with SCRAM-SHA-256 mechanism
        listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
        username="admin" \
        password="admin-secret";

        # SASL over SSL with SCRAM-SHA-512 mechanism
        listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
        username="admin" \
        password="admin-secret";

        # SASL over SSL with GSSAPI mechanism
        listener.name.sasl_ssl.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
        useKeyTab=true \
        storeKey=true \
        keyTab="/etc/krb5-keytabs/kafka.service.keytab" \
        principal="kafka/192.168.1.100@REALM";

        # PLAINTEXT SASL with PLAIN mechanism
        listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
        username="admin" \
        password="admin-secret" \
        user_alice="alice-secret";

        # PLAINTEXT SASL with SCRAM-SHA-256 mechanism
        listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
        username="admin" \
        password="admin-secret";

        # PLAINTEXT SASL with SCRAM-SHA-512 mechanism
        listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
        username="admin" \
        password="admin-secret";

        # PLAINTEXT SASL with GSSAPI mechanism
        listener.name.sasl_plaintext.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
        useKeyTab=true \
        storeKey=true \
        keyTab="/etc/krb5-keytabs/kafka.service.keytab" \
        principal="kafka/192.168.1.100@REALM";

3. Kerberos setup::

   librdkafka (inside RGW) builds the broker SPN (Service Principal Name, basically the 
   principal that represents the Kafka broker) as ``kafka/<broker-host>@REALM``, 
   where ``<broker-host>`` is whatever appears in ``advertised.listeners``.
   Since step 2 uses the broker's IP in ``advertised.listeners``, the 
   broker principal must be tied to that same IP (or hostname — they must match).

   The examples below use realm ``REALM.COM`` and broker IP ``192.168.1.100``.
   Substitute your own values.

   a. Install Kerberos and SASL/GSSAPI packages.

      Ubuntu/Debian::

              sudo apt update
              sudo DEBIAN_FRONTEND=noninteractive apt install -y krb5-kdc krb5-admin-server krb5-user \
                                  libsasl2-modules-gssapi-mit libsasl2-modules

      Fedora/RHEL::

              sudo dnf install -y krb5-server krb5-workstation cyrus-sasl-gssapi

   b. Configure ``/etc/krb5.conf``.

      Reference contents::

              [libdefaults]
                        default_realm = REALM.COM
                        dns_lookup_realm = false
                        dns_lookup_kdc = false
                        rdns = false
                        forwardable = true
                        ticket_lifetime = 24h
                        renew_lifetime = 7d
              [realms]
                        REALM.COM = {
                                kdc = 192.168.1.100
                                admin_server = 192.168.1.100
                        }

              [domain_realm]
                        .realm.com = REALM.COM
                        realm.com = REALM.COM

   c. Bootstrap the KDC (first-time setup only)::

              sudo kdb5_util create -s -r REALM.COM                                # set + remember master password
              echo '*/admin@REALM.COM    *' | sudo tee /etc/krb5kdc/kadm5.acl      # allow admin principal to do anything

              # start and enable KDC and admin server
              Ubuntu/Debian::
                      sudo systemctl enable --now krb5-kdc krb5-admin-server
                      sudo systemctl status krb5-kdc krb5-admin-server

              Fedora/RHEL::
                      sudo systemctl enable --now krb5kdc kadmin
                      sudo systemctl status krb5kdc kadmin

   d. Create the broker and RGW principals + keytabs::

              sudo mkdir -p /etc/krb5-keytabs                                      # to store the keytabs
              sudo kadmin.local -q "addprinc admin/admin"
              sudo kadmin.local -q "addprinc -randkey kafka/192.168.1.100"
              sudo kadmin.local -q "addprinc -randkey rgw/192.168.1.100"
              sudo kadmin.local -q "ktadd -k /etc/krb5-keytabs/kafka.service.keytab kafka/192.168.1.100"
              sudo kadmin.local -q "ktadd -k /etc/krb5-keytabs/rgw.service.keytab rgw/192.168.1.100"

              sudo chmod 640 /etc/krb5-keytabs/*.keytab
              sudo chown root:$USER /etc/krb5-keytabs/*.keytab                     # Make keytabs readable by the user that runs the Kafka broker 
                                                                                   # AND the user that runs vstart (often the same user)

   e. Verify the keytabs and get a TGT for the RGW principal::

              klist -kt /etc/krb5-keytabs/kafka.service.keytab
              klist -kt /etc/krb5-keytabs/rgw.service.keytab
              kinit -kt /etc/krb5-keytabs/rgw.service.keytab rgw/192.168.1.100@REALM.COM
              klist                                                                # should show a valid TGT

   f. Make sure to add the Kafka principal, RGW principal, and keytab to your ``bntests.conf``.

4. Start Zookeeper and Kafka.

5. Start RGW vstart cluster with cleartext parameter set to true::

        cd /path/to/ceph/build
        MON=1 OSD=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d -o "rgw_allow_notification_secrets_in_cleartext=true" -o "rgw_kafka_sasl_kerberos_service_name=kafka"

6. Run the tests::

        cd /path/to/ceph
        KAFKA_DIR=/path/to/kafka BNTESTS_CONF=/path/to/bntests.conf python -m pytest -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -m 'kafka_security_test'

        For GSSAPI tests, ensure a valid ticket cache already exists for RGW principal
        (for example, via ``kinit``).

==============
RabbitMQ Tests
==============

You need to install RabbitMQ, check supported platforms: https://www.rabbitmq.com/docs/platforms. For example, for Fedora::

        sudo dnf install rabbitmq-server

Then you need to run the following command::

        sudo chkconfig rabbitmq-server on

Update rabbitmq-server configuration to allow access to the guest user from anywhere on the network. Uncomment or add line to rabbirmq configuration, usually `/etc/rabbitmq/rabbirmq.comf`::

        loopback_user.guest = false

Finally, to start the RabbitMQ server you need to run the following command::

        sudo systemctl start rabbitmq-server

To confirm that the RabbitMQ server is running you can run the following command to check the status of the server::

        sudo systemctl status rabbitmq-server

After running `vstart.sh` and RabbitMQ server you're ready to run the AMQP tests::

        BNTESTS_CONF=bntests.conf python -m pytest -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -m 'amqp_test'

After running the tests you need to stop the vstart cluster (``/path/to/ceph/src/stop.sh``) and the RabbitMQ server by running the following command::

        sudo systemctl stop rabbitmq-server

To run the RabbitMQ SSL security tests use the following::

        BNTESTS_CONF=bntests.conf python -m pytest -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -m 'amqp_ssl_test'

During these tests, the test script will restart the RabbitMQ server with the correct security configuration (``sudo`` privileges will be needed).
For that reason it is not recommended to run the `amqp_ssl_test` tests, that assumes a manually configured rabbirmq server, in the same run as `amqp_test` tests, 
that assume the rabbitmq daemon running on the host as a service.
