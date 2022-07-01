# SNMP schema
To show the [OID](https://en.wikipedia.org/wiki/Object_identifier)'s supported by the MIB, use the snmptranslate command. Here's an example:
```
snmptranslate -Pu -Tz -M ~/git/ceph/monitoring/snmp:/usr/share/snmp/mibs -m CEPH-MIB
```
*The `snmptranslate` command is in the net-snmp-utils package*

The MIB provides a NOTIFICATION only implementation since ceph doesn't have an SNMP
agent feature.

## Integration
The SNMP MIB is has been aligned to the Prometheus rules. Any rule that defines a 
critical alert should have a corresponding oid in the CEPH-MIB.txt file. To generate
an SNMP notification, you must use an SNMP gateway that the Prometheus Alertmanager
service can forward alerts through to, via it's webhooks feature.

&nbsp;

## SNMP Gateway
The recommended SNMP gateway is https://github.com/maxwo/snmp_notifier. This is a widely
used and generic SNMP gateway implementation written in go. It's usage (syntax and
parameters) is very similar to Prometheus, AlertManager and even node-exporter.

&nbsp;
## SNMP OIDs
The main components of the Ceph MIB is can be broken down into discrete areas


```
internet private enterprise   ceph   ceph    Notifications   Prometheus  Notification
                               org  cluster   (alerts)         source      Category
1.3.6.1   .4     .1          .50495   .1        .2               .1         .2  (Ceph Health)
                                                                            .3  (MON)
                                                                            .4  (OSD)
                                                                            .5  (MDS)
                                                                            .6  (MGR)
                                                                            .7  (PGs)
                                                                            .8  (Nodes)
                                                                            .9  (Pools)
                                                                            .10  (Rados)
                                                                            .11 (cephadm)
                                                                            .12 (prometheus)

```
Individual alerts are placed within the appropriate alert category. For example, to add
a notification relating to a MGR issue, you would use the oid 1.3.6.1.4.1.50495.1.2.1.6.x

The SNMP gateway also adds additional components to the SNMP notification ;

| Suffix | Description |
|--------|-------------|
| .1 | The oid |
| .2 | Severity of the alert. When an alert is resolved, severity is 'info', and the description is set to Status:OK|
| .3 | Text of the alert(s) | 
