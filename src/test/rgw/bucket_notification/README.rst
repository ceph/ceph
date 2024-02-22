==========================
 Bucket Notification Tests
==========================

You will need to use the sample configuration file named ``bntests.conf.SAMPLE``
that has been provided at ``/path/to/ceph/src/test/rgw/bucket_notification/``. You can also copy this file to the directory where you are
running the tests and modify it if needed. This file can be used to run the bucket notification tests on a Ceph cluster started
with the `vstart.sh` script.
For the tests covering Kafka and RabbitMQ security, the RGW will need to accept use/password without TLS connection between the client and the RGW.
So, the cluster will have to be started with the following ``rgw_allow_notification_secrets_in_cleartext`` parameter set to ``true``.


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

        BNTESTS_CONF=bntests.conf python -m nose -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -a 'kafka_test'

--------------------
Kafka Security Tests
--------------------

First, make sure that vstart was initiated with the following ``rgw_allow_notification_secrets_in_cleartext`` parameter set to ``true``::

        MON=1 OSD=1 MDS=0 MGR=1 RGW=1 ../src/vstart.sh -n -d -o "rgw_allow_notification_secrets_in_cleartext=true"

Then you should run the ``kafka-security.sh`` script inside the Kafka directory::

        cd /path/to/kafka/
        /path/to/ceph/src/test/rgw/bucket_notification/kafka-security.sh

Then make sure the Kafka server properties file (``/path/to/kafka/config/server.properties``) has the following lines::


        # all listeners
        listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093,SASL_SSL://localhost:9094,SASL_PLAINTEXT://localhost:9095

        # SSL configuration matching the kafka-security.sh script
        ssl.keystore.location=./server.keystore.jks
        ssl.keystore.password=mypassword
        ssl.key.password=mypassword
        ssl.truststore.location=./server.truststore.jks
        ssl.truststore.password=mypassword

        # SASL mechanisms
        sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256

        # SASL over SSL with SCRAM-SHA-256 mechanism
        listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
          username="alice" \
          password="alice-secret" \
          user_alice="alice-secret";

        # SASL over SSL with PLAIN mechanism
        listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
          username="alice" \
          password="alice-secret" \
          user_alice="alice-secret";

        # PLAINTEXT SASL with SCRAM-SHA-256 mechanism
        listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
          username="alice" \
          password="alice-secret" \
          user_alice="alice-secret";

        # PLAINTEXT SASL with PLAIN mechanism
        listener.name.sasl_plaintext.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
          username="alice" \
          password="alice-secret" \
          user_alice="alice-secret";


And restart the Kafka server. Once both Zookeeper and Kafka are up, run the following command (for the SASL SCRAM test) from the Kafka directory::

        bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=alice-secret],SCRAM-SHA-512=[password=alice-secret]' --entity-type users --entity-name alice


To run the Kafka security test, you also need to provide the test with the location of the Kafka directory::

        KAFKA_DIR=/path/to/kafka BNTESTS_CONF=bntests.conf python -m nose -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -a 'kafka_security_test'

==============
RabbitMQ Tests
==============

You need to install RabbitMQ in the following way::

        sudo dnf install rabbitmq-server

Then you need to run the following command::

        sudo chkconfig rabbitmq-server on

Finally, to start the RabbitMQ server you need to run the following command::

        sudo /sbin/service rabbitmq-server start

To confirm that the RabbitMQ server is running you can run the following command to check the status of the server::

        sudo /sbin/service rabbitmq-server status

After running `vstart.sh` and RabbitMQ server you're ready to run the AMQP tests::

        BNTESTS_CONF=bntests.conf python -m nose -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -a 'amqp_test'

After running the tests you need to stop the vstart cluster (``/path/to/ceph/src/stop.sh``) and the RabbitMQ server by running the following command::

        sudo /sbin/service rabbitmq-server stop

To run the RabbitMQ SSL security tests use the following::

        BNTESTS_CONF=bntests.conf python -m nose -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py -v -a 'amqp_ssl_test'

During these tests, the test script will restart the RabbitMQ server with the correct security configuration (``sudo`` privileges will be needed).
For that reason it is not recommended to run the `amqp_ssl_test` tests, that assumes a manually configured rabbirmq server, in the same run as `amqp_test` tests, 
that assume the rabbitmq daemon running on the host as a service.

