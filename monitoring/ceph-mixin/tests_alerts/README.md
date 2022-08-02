
## Alert Rule Standards

The alert rules should adhere to the following principles
- each alert must have a unique name
- each alert should define a common structure
  - labels : must contain severity and type
  - annotations : must provide description
  - expr : must define the promql expression 
  - alert : defines the alert name 
- alerts that have a corresponding section within docs.ceph.com must include a 
  documentation field in the annotations section
- critical alerts should declare an oid in the labels section
- critical alerts should have a corresponding entry in the Ceph MIB

&nbsp;
## Testing Prometheus Rules
Once you have updated the `ceph_default_alerts.yml` file, you should use the 
`validate_rules.py` script directly, or via `tox` to ensure the format of any update 
or change aligns to our rule structure guidelines. The validate_rules.py script will
process the rules and look for any configuration anomalies and output a report if
problems are detected.

Here's an example run, to illustrate the format and the kinds of issues detected.

```
[paul@myhost tests]$ ./validate_rules.py 

Checking rule groups
        cluster health : ..
        mon            : E.W..
        osd            : E...W......W.E..
        mds            : WW
        mgr            : WW
        pgs            : ..WWWW..
        nodes          : .EEEE
        pools          : EEEW.
        healthchecks   : .
        cephadm        : WW.
        prometheus     : W
        rados          : W

Summary

Rule file             : ../alerts/ceph_default_alerts.yml
Unit Test file        : test_alerts.yml

Rule groups processed :  12
Rules processed       :  51
Rule errors           :  10
Rule warnings         :  16
Rule name duplicates  :   0
Unit tests missing    :   4

Problem Report

  Group       Severity  Alert Name                                          Problem Description
  -----       --------  ----------                                          -------------------
  cephadm     Warning   Cluster upgrade has failed                          critical level alert is missing an SNMP oid entry
  cephadm     Warning   A daemon managed by cephadm is down                 critical level alert is missing an SNMP oid entry
  mds         Warning   Ceph Filesystem damage detected                     critical level alert is missing an SNMP oid entry
  mds         Warning   Ceph Filesystem switched to READ ONLY               critical level alert is missing an SNMP oid entry
  mgr         Warning   mgr module failure                                  critical level alert is missing an SNMP oid entry
  mgr         Warning   mgr prometheus module is not active                 critical level alert is missing an SNMP oid entry
  mon         Error     Monitor down, quorum is at risk                     documentation link error: #mon-downwah not found on the page
  mon         Warning   Ceph mon disk space critically low                  critical level alert is missing an SNMP oid entry
  nodes       Error     network packets dropped                             invalid alert structure. Missing field: for
  nodes       Error     network packet errors                               invalid alert structure. Missing field: for
  nodes       Error     storage filling up                                  invalid alert structure. Missing field: for
  nodes       Error     MTU Mismatch                                        invalid alert structure. Missing field: for
  osd         Error     10% OSDs down                                       invalid alert structure. Missing field: for
  osd         Error     Flapping OSD                                        invalid alert structure. Missing field: for
  osd         Warning   OSD Full                                            critical level alert is missing an SNMP oid entry
  osd         Warning   Too many devices predicted to fail                  critical level alert is missing an SNMP oid entry
  pgs         Warning   Placement Group (PG) damaged                        critical level alert is missing an SNMP oid entry
  pgs         Warning   Recovery at risk, cluster too full                  critical level alert is missing an SNMP oid entry
  pgs         Warning   I/O blocked to some data                            critical level alert is missing an SNMP oid entry
  pgs         Warning   Cluster too full, automatic data recovery impaired  critical level alert is missing an SNMP oid entry
  pools       Error     pool full                                           invalid alert structure. Missing field: for
  pools       Error     pool filling up (growth forecast)                   invalid alert structure. Missing field: for
  pools       Error     Ceph pool is too full for recovery/rebalance        invalid alert structure. Missing field: for
  pools       Warning   Ceph pool is full - writes blocked                  critical level alert is missing an SNMP oid entry
  prometheus  Warning   Scrape job is missing                               critical level alert is missing an SNMP oid entry
  rados       Warning   Data not found/missing                              critical level alert is missing an SNMP oid entry

Unit tests are incomplete. Tests missing for the following alerts;
  - Placement Group (PG) damaged
  - OSD Full
  - storage filling up
  - pool filling up (growth forecast)

```
