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

Jaeger requires a database for the traces. We use Elasticsearch (version 6)
by default.

To deploy the Jaeger service when not using your own Elasticsearch (deploys the Jaeger
service with a new Elasticsearch container):

.. prompt:: bash #

   ceph orch apply jaeger

To deploy the Jaeger service with an existing Elasticsearch cluster:

.. prompt:: bash #

   ceph orch apply jaeger --es_nodes=ip:port,...

The Jaeger service runs as a single daemon that includes the collector and the
query UI.
Deployments from older versions which used multiple containers are migrated automatically
to the single ``jaeger`` service. Placement is inherited from the jaeger-collector placement;
agent and query placements are discarded.

