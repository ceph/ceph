================
Tracing Services
================

.. _cephadm-tracing:


Jaeger Tracing
==============

Ceph uses Jaeger as the tracing backend. in order to use tracing, we need to deploy those services.

Further details on tracing in ceph:

`Ceph Tracing documentation <https://docs.ceph.com/en/latest/jaegertracing/#jaeger-distributed-tracing/>`_

Deployment
==========

Jaeger services consist of 3 services:

1. Jaeger Agent

2. Jaeger Collector

3. Jaeger Query

Jaeger requires a database for the traces. we use ElasticSearch by default.


To deploy jaeger tracing service, without ElasticSearch:

#. Deploy jaeger services, with a new elasticsearch container:

    .. prompt:: bash #

        ceph orch apply jaeger


#. Deploy jaeger services, with existing elasticsearch cluster:

     .. prompt:: bash #

        ceph orch apply jaeger -es_nodes=ip:port,..

