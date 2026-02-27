## Prometheus Monitoring Mixin for Ceph
A set of Grafana dashboards and Prometheus alerts for Ceph.

All the Grafana dashboards are already generated in the `dashboards_out`
directory and alerts in the `prometheus_alerts.yml` file.

You can use the Grafana dashboards and alerts with Jsonnet like any other
prometheus mixin. You can find more resources about mixins in general on
[monitoring.mixins.dev](https://monitoring.mixins.dev/).

-------

### Grafana dashboards for Ceph
In `dashboards_out` you can find a collection of
[Grafana](https://grafana.com/grafana) dashboards for Ceph Monitoring.

These dashboards are based on metrics collected
from [prometheus](https://prometheus.io/) scraping the [prometheus mgr
plugin](http://docs.ceph.com/en/latest/mgr/prometheus/) and the
[node_exporter (0.17.0)](https://github.com/prometheus/node_exporter).


### Prometheus alerts
In `prometheus_alerts.libsonnet` you'll find a set of Prometheus
alert rules that should provide a decent set of default alerts for a
Ceph cluster. After building them with jsonnet put this file in place according to your Prometheus
configuration (wherever the `rules` configuration stanza points).


### SNMP
Ceph provides a MIB (CEPH-PROMETHEUS-ALERT-MIB.txt) to support sending
Prometheus alerts to an SNMP management platform. The translation from
Prometheus alert to SNMP trap requires the Prometheus alert to contain an OID
that maps to a definition within the MIB. When making changes to the Prometheus
alert rules file, developers should include any necessary changes to the MIB.

### Multi-cluster support
Ceph-mixin supports dashboards and alerts across multiple clusters. 
To enable this feature you need to configure the following in `config.libsonnnet`:

```
showMultiCluster: true,
clusterLabel: '<your cluster label>',
```
----------------

### Building from Jsonnet

#### Method 1: System Packages (Recommended for most users)

```bash
sudo dnf install jsonnet jsonnet-bundler  # RHEL/Fedora
sudo apt-get install jsonnet jsonnet-bundler  # Ubuntu/Debian

# Install dependencies
jb install

# Generate all dashboards and alerts
make generate

# Run tests
make test
```

#### Method 2: Local Build (For CI/CD or no root access)

```bash
./jsonnet-bundler-build.sh

# Install dependencies using local jb
./jb install

# Generate all dashboards and alerts  
make generate

# Run tests
make test
```

### Ceph Mixin Development Commands

```bash
make all        # Format, generate, lint, and test
make fmt        # Format Jsonnet files
make generate   # Regenerate JSON dashboards and alerts from templates
make lint       # Run linters
make test       # Run all tests
make vendor     # Install dependencies
make help       # List all available commands
```

### Supported Versions
* jsonnet: v0.18.0+
* jsonnet-builder: 0.4.0+
* grafonnet-lib: always uses latest `master` branch
* alertmanager: 0.16.2+
* prometheus: v2.33.4+
