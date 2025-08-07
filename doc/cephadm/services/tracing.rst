================
Tracing Services
================

.. _cephadm-tracing:


Jaeger Tracing
==============

Ceph uses Jaeger as its tracing backend. In order to use tracing, we need to
deploy those services.

For further details on tracing in Ceph, see
:ref:`Ceph tracing documentation <jaegertracing>`.

Deployment
==========

Jaeger tracing consists of 3 services:

#. Jaeger Agent
#. Jaeger Collector
#. Jaeger Query

Jaeger requires a database for the traces. We use ElasticSearch (version 6)
by default.

To deploy Jaeger services when not using your own ElasticSearch (deploys
all 3 services with a new ElasticSearch container):

.. prompt:: bash #

   ceph orch apply jaeger

To deploy Jaeger services with an existing ElasticSearch cluster and
an existing Jaeger query (deploys agents and collectors only):

.. prompt:: bash #

   ceph orch apply jaeger --without-query --es_nodes=ip:port,..

