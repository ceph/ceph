.. _mgr-cephadm-monitoring:

Monitoring Services
===================

Ceph Dashboard uses `Prometheus <https://prometheus.io/>`_, `Grafana
<https://grafana.com/>`_, and related tools to store and visualize detailed
metrics on cluster utilization and performance.  Ceph users have three options:

#. Have cephadm deploy and configure these services.  This is the default
   when bootstrapping a new cluster unless the ``--skip-monitoring-stack``
   option is used.
#. Deploy and configure these services manually.  This is recommended for users
   with existing prometheus services in their environment (and in cases where
   Ceph is running in Kubernetes with Rook).
#. Skip the monitoring stack completely.  Some Ceph dashboard graphs will
   not be available.

The monitoring stack consists of `Prometheus <https://prometheus.io/>`_,
Prometheus exporters (:ref:`mgr-prometheus`, `Node exporter
<https://prometheus.io/docs/guides/node-exporter/>`_), `Prometheus Alert
Manager <https://prometheus.io/docs/alerting/alertmanager/>`_ and `Grafana
<https://grafana.com/>`_.

.. note::

  Prometheus' security model presumes that untrusted users have access to the
  Prometheus HTTP endpoint and logs. Untrusted users have access to all the
  (meta)data Prometheus collects that is contained in the database, plus a
  variety of operational and debugging information.

  However, Prometheus' HTTP API is limited to read-only operations.
  Configurations can *not* be changed using the API and secrets are not
  exposed. Moreover, Prometheus has some built-in measures to mitigate the
  impact of denial of service attacks.

  Please see `Prometheus' Security model
  <https://prometheus.io/docs/operating/security/>` for more detailed
  information.

Deploying monitoring with cephadm
---------------------------------

By default, bootstrap will deploy a basic monitoring stack.  If you
did not do this (by passing ``--skip-monitoring-stack``, or if you
converted an existing cluster to cephadm management, you can set up
monitoring by following the steps below.

#. Enable the prometheus module in the ceph-mgr daemon.  This exposes the internal Ceph metrics so that prometheus can scrape them.

   .. code-block:: bash

     ceph mgr module enable prometheus

#. Deploy a node-exporter service on every node of the cluster.  The node-exporter provides host-level metrics like CPU and memory utilization.

   .. code-block:: bash

     ceph orch apply node-exporter '*'

#. Deploy alertmanager

   .. code-block:: bash

     ceph orch apply alertmanager 1

#. Deploy prometheus.  A single prometheus instance is sufficient, but
   for HA you may want to deploy two.

   .. code-block:: bash

     ceph orch apply prometheus 1    # or 2

#. Deploy grafana

   .. code-block:: bash

     ceph orch apply grafana 1

Cephadm takes care of the configuration of Prometheus, Grafana, and Alertmanager
automatically.

However, there is one exception to this rule. In a some setups, the Dashboard
user's browser might not be able to access the Grafana URL configured in Ceph
Dashboard. One such scenario is when the cluster and the accessing user are each
in a different DNS zone.

For this case, there is an extra configuration option for Ceph Dashboard, which
can be used to configure the URL for accessing Grafana by the user's browser.
This value will never be altered by cephadm. To set this configuration option,
issue the following command::

  $ ceph dashboard set-grafana-frontend-api-url <grafana-server-api>

It may take a minute or two for services to be deployed.  Once
completed, you should see something like this from ``ceph orch ls``

.. code-block:: console

  $ ceph orch ls
  NAME           RUNNING  REFRESHED  IMAGE NAME                                      IMAGE ID        SPEC
  alertmanager       1/1  6s ago     docker.io/prom/alertmanager:latest              0881eb8f169f  present
  crash              2/2  6s ago     docker.io/ceph/daemon-base:latest-master-devel  mix           present
  grafana            1/1  0s ago     docker.io/pcuzner/ceph-grafana-el8:latest       f77afcf0bcf6   absent
  node-exporter      2/2  6s ago     docker.io/prom/node-exporter:latest             e5a616e4b9cf  present
  prometheus         1/1  6s ago     docker.io/prom/prometheus:latest                e935122ab143  present

Configuring SSL/TLS for Grafana
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``cephadm`` will deploy Grafana using the certificate defined in the ceph
key/value store. If a certificate is not specified, ``cephadm`` will generate a
self-signed certificate during deployment of the Grafana service.

A custom certificate can be configured using the following commands.

.. code-block:: bash

  ceph config-key set mgr/cephadm/grafana_key -i $PWD/key.pem
  ceph config-key set mgr/cephadm/grafana_crt -i $PWD/certificate.pem

If you already deployed Grafana, you need to ``reconfig`` the service for the
configuration to be updated.

.. code-block:: bash

  ceph orch reconfig grafana

The ``reconfig`` command also takes care of setting the right URL for Ceph
Dashboard.

Using custom images
~~~~~~~~~~~~~~~~~~~

It is possible to install or upgrade monitoring components based on other
images.  To do so, the name of the image to be used needs to be stored in the
configuration first.  The following configuration options are available.

- ``container_image_prometheus``
- ``container_image_grafana``
- ``container_image_alertmanager``
- ``container_image_node_exporter``

Custom images can be set with the ``ceph config`` command

.. code-block:: bash

     ceph config set mgr mgr/cephadm/<option_name> <value>

For example

.. code-block:: bash

     ceph config set mgr mgr/cephadm/container_image_prometheus prom/prometheus:v1.4.1

.. note::

     By setting a custom image, the default value will be overridden (but not
     overwritten).  The default value changes when updates become available.
     By setting a custom image, you will not be able to update the component
     you have set the custom image for automatically.  You will need to
     manually update the configuration (image name and tag) to be able to
     install updates.

     If you choose to go with the recommendations instead, you can reset the
     custom image you have set before.  After that, the default value will be
     used again.  Use ``ceph config rm`` to reset the configuration option

     .. code-block:: bash

          ceph config rm mgr mgr/cephadm/<option_name>

     For example

     .. code-block:: bash

          ceph config rm mgr mgr/cephadm/container_image_prometheus

Using custom configuration files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By overriding cephadm templates, it is possible to completely customize the
configuration files for monitoring services.

Internally, cephadm already uses `Jinja2
<https://jinja.palletsprojects.com/en/2.11.x/>`_ templates to generate the
configuration files for all monitoring components. To be able to customize the
configuration of Prometheus, Grafana or the Alertmanager it is possible to store
a Jinja2 template for each service that will be used for configuration
generation instead. This template will be evaluated every time a service of that
kind is deployed or reconfigured. That way, the custom configuration is
preserved and automatically applied on future deployments of these services.

.. note::

  The configuration of the custom template is also preserved when the default
  configuration of cephadm changes. If the updated configuration is to be used,
  the custom template needs to be migrated *manually*.

Option names
""""""""""""

The following templates for files that will be generated by cephadm can be
overridden. These are the names to be used when storing with ``ceph config-key
set``:

- ``services/alertmanager/alertmanager.yml``
- ``services/grafana/ceph-dashboard.yml``
- ``services/grafana/grafana.ini``
- ``services/prometheus/prometheus.yml``

You can look up the file templates that are currently used by cephadm in
``src/pybind/mgr/cephadm/templates``:

- ``services/alertmanager/alertmanager.yml.j2``
- ``services/grafana/ceph-dashboard.yml.j2``
- ``services/grafana/grafana.ini.j2``
- ``services/prometheus/prometheus.yml.j2``

Usage
"""""

The following command applies a single line value:

.. code-block:: bash

  ceph config-key set mgr/cephadm/<option_name> <value>

To set contents of files as template use the ``-i`` argument:

.. code-block:: bash

  ceph config-key set mgr/cephadm/<option_name> -i $PWD/<filename>

.. note::

  When using files as input to ``config-key`` an absolute path to the file must
  be used.


Then the configuration file for the service needs to be recreated.
This is done using `reconfig`. For more details see the following example.

Example
"""""""

.. code-block:: bash

  # set the contents of ./prometheus.yml.j2 as template
  ceph config-key set mgr/cephadm/services/prometheus/prometheus.yml \
    -i $PWD/prometheus.yml.j2

  # reconfig the prometheus service
  ceph orch reconfig prometheus

Disabling monitoring
--------------------

If you have deployed monitoring and would like to remove it, you can do
so with

.. code-block:: bash

  ceph orch rm grafana
  ceph orch rm prometheus --force   # this will delete metrics data collected so far
  ceph orch rm node-exporter
  ceph orch rm alertmanager
  ceph mgr module disable prometheus


Deploying monitoring manually
-----------------------------

If you have an existing prometheus monitoring infrastructure, or would like
to manage it yourself, you need to configure it to integrate with your Ceph
cluster.

* Enable the prometheus module in the ceph-mgr daemon

  .. code-block:: bash

     ceph mgr module enable prometheus

  By default, ceph-mgr presents prometheus metrics on port 9283 on each host
  running a ceph-mgr daemon.  Configure prometheus to scrape these.

* To enable the dashboard's prometheus-based alerting, see :ref:`dashboard-alerting`.

* To enable dashboard integration with Grafana, see :ref:`dashboard-grafana`.

Enabling RBD-Image monitoring
---------------------------------

Due to performance reasons, monitoring of RBD images is disabled by default. For more information please see
:ref:`prometheus-rbd-io-statistics`. If disabled, the overview and details dashboards will stay empty in Grafana
and the metrics will not be visible in Prometheus.
