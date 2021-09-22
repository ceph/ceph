## Prometheus related bits

### Alerts
In monitoring/prometheus/alerts you'll find a set of Prometheus alert rules that
should provide a decent set of default alerts for a Ceph cluster. Just put this
file in a place according to your Prometheus configuration (wherever the `rules`
configuration stanza points).

### SNMP
Ceph provides a MIB (CEPH-PROMETHEUS-ALERT-MIB.txt) to support sending prometheus
alerts through to an SNMP management platform. The translation from prometheus
alert to SNMP trap requires the prometheus alert to contain an OID that maps to
a definition within the MIB. When making changes to the prometheus alert rules
file, developers should include any necessary changes to the MIB.
