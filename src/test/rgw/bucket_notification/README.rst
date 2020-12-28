============================
 Bucket Notification tests
============================

You will need to use the sample configuration file named ``bntests.conf.SAMPLE`` 
that has been provided at ``/path/to/ceph/src/test/rgw/bucket_notification/``. You can also copy this file to the directory where you are
running the tests and modify it if needed. This file can be used to run the bucket notification tests on a Ceph cluster started 
with vstart.

You also need to install Kafka which can be done by downloading and unzipping from the following::

        https://archive.apache.org/dist/kafka/2.6.0/kafka-2.6.0-src.tgz

Then inside the kafka config directory (``/path/to/kafka-2.6.0-src/config/``) you need to create a file named ``kafka_server_jaas.conf``
with the following content::

        KafkaClient {
          org.apache.kafka.common.security.plain.PlainLoginModule required
          username="alice"
          password="alice-secret";
        };

After creating this above file run the following command in kafka directory (``/path/to/kafka-2.6.0-src/``)::

        ./gradlew jar -PscalaVersion=2.13.2

After following the above steps next is you need to start the Zookeeper and Kafka services.
Here's the commands which can be used to start these services. For starting 
Zookeeper service run::

        bin/zookeeper-server-start.sh config/zookeeper.properties

and then run to start the Kafka service::

        bin/kafka-server-start.sh config/server.properties

If you want to run Zookeeper and Kafka services in background add ``-daemon`` at the end of the command like::

        bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

and::

        bin/kafka-server-start.sh -daemon config/server.properties

After starting vstart, zookeeper and kafka services you're ready to run the tests::

        BNTESTS_CONF=bntests.conf python -m nose -s /path/to/ceph/src/test/rgw/bucket_notification/test_bn.py
