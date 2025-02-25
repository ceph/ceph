## Prometheus Monitoring Mixin for Ceph
A set of Grafana dashboards and Prometheus alerts for Ceph.

All the Grafana dashboards are already generated in the `dashboards_out`
directory and alerts in the `prometheus_alerts.yml` file.

You can use the Grafana dashboards and alerts with Jsonnet like any other
prometheus mixin. You can find more resources about mixins in general on
[monitoring.mixins.dev](https://monitoring.mixins.dev/).

### Grafana dashboards for Ceph
In `dashboards_out` you can find a collection of
[Grafana](https://grafana.com/grafana) dashboards for Ceph Monitoring.

These dashboards are based on metrics collected
from [prometheus](https://prometheus.io/) scraping the [prometheus mgr
plugin](http://docs.ceph.com/en/latest/mgr/prometheus/) and the
[node_exporter (0.17.0)](https://github.com/prometheus/node_exporter).


##### Recommended versions: 
-grafana 8.3.5
    -grafana-piechart-panel 1.6.2
    -grafana-status-panel 1.0.11

#### Requirements

- [Status Panel](https://grafana.com/plugins/vonage-status-panel) installed on
 your Grafana instance
- [Pie Chart Panel](https://grafana.com/grafana/plugins/grafana-piechart-panel/)
 installed on your Grafana instance


### Prometheus alerts
In `prometheus_alerts.libsonnet` you'll find a set of Prometheus
alert rules that should provide a decent set of default alerts for a
Ceph cluster. After building them with jsonnet put this file in place according to your Prometheus
configuration (wherever the `rules` configuration stanza points).

### Multi-cluster support
Ceph-mixin supports dashboards and alerts across multiple clusters. 
To enable this feature you need to configure the following in `config.libsonnnet`:

```
showMultiCluster: true,
clusterLabel: '<your cluster label>',
```

##### Recommended versions: 
-prometheus v2.33.4

#### SNMP
Ceph provides a MIB (CEPH-PROMETHEUS-ALERT-MIB.txt) to support sending
Prometheus alerts to an SNMP management platform. The translation from
Prometheus alert to SNMP trap requires the Prometheus alert to contain an OID
that maps to a definition within the MIB. When making changes to the Prometheus
alert rules file, developers should include any necessary changes to the MIB.


##### Recommended: 
-alertmanager 0.16.2

### Building from Jsonnet

- Install [jsonnet](https://jsonnet.org/) (at least v0.18.0)
    - By installing the package `jsonnet` in most of the distro and
      `golang-github-google-jsonnet` in fedora
- Install [jsonnet-bundler](https://github.com/jsonnet-bundler/jsonnet-bundler)

To rebuild all the generated files, you can run `tox -egrafonnet-fix`.

The jsonnet code located in this directory depends on some Jsonnet third party
libraries. To update those libraries you can run `jb update` and then update
the generated files using `tox -egrafonnet-fix`.

### Building alerts from `prometheus_alerts.libsonnet`

To rebuild the `prometheus_alerts.yml` file from the corresponding libsonnet,
you can run `tox -ealerts-fix`.


##### Any upgrade or downgrade to different major versions of the recommended tools mentioned above is not supported.
