Feature: OSD Overview

Scenario: "Test OSD onode Hits Ratio"
  Given the following series:
    | metrics | values |
    | ceph_bluestore_onode_hits{ceph_daemon="osd.0",instance="ceph:9283",job="ceph",cluster="mycluster"} | 5255 |
    | ceph_bluestore_onode_hits{ceph_daemon="osd.1",instance="ceph:9283",job="ceph",cluster="mycluster"} | 5419 |
    | ceph_bluestore_onode_hits{ceph_daemon="osd.2",instance="ceph:9283",job="ceph",cluster="mycluster"} | 5242 |
    | ceph_bluestore_onode_misses{ceph_daemon="osd.0",instance="ceph:9283",job="ceph",cluster="mycluster"} | 202 |
    | ceph_bluestore_onode_misses{ceph_daemon="osd.1",instance="ceph:9283",job="ceph",cluster="mycluster"} | 247 |
    | ceph_bluestore_onode_misses{ceph_daemon="osd.2",instance="ceph:9283",job="ceph",cluster="mycluster"} | 234 |
  Then Grafana panel `OSD onode Hits Ratio` with legend `EMPTY` shows:
    | metrics | values |
    | {} | 9.588529429483704E-01 |

