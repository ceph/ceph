## Grafana dashboards for Ceph

Here you can find a collection of [Grafana](https://grafana.com/grafana)
dashboards for Ceph Monitoring. These dashboards are based on metrics collected
from [prometheus](https://prometheus.io/) scraping the [prometheus mgr
plugin](http://docs.ceph.com/docs/master/mgr/prometheus/) and the
[node_exporter](https://github.com/prometheus/node_exporter).

### Other requirements

- Luminous 12.2.5 or newer
- [Status Panel](https://grafana.com/plugins/vonage-status-panel) installed
- node_exporter 0.15.x and 0.16.x are supported (host details and hosts
overview dashboards)
