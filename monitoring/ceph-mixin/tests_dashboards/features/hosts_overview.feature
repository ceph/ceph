Feature: Hosts Overview Dashboard

Scenario: "Test network load succeeds"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{job="node",instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_receive_bytes{job="node",instance="127.0.0.1", device="eth2"} | 10 100 |
    | node_network_transmit_bytes{job="node",instance="127.0.0.1", device="eth1"} | 10 100 |
    | node_network_transmit_bytes{job="node",instance="127.0.0.1", device="eth2"} | 10 100 |
  When variable `osd_hosts` is `127.0.0.1`
  Then Grafana panel `Network Load` with legend `EMPTY` shows:
    | metrics | values |
    | {} | 6 |

Scenario: "Test network load with bonding succeeds"
  Given the following series:
    | metrics | values |
    | node_network_receive_bytes{job="node",instance="127.0.0.1", device="eth1"} | 10 100 200 |
    | node_network_receive_bytes{job="node",instance="127.0.0.1", device="eth2"} | 10 100 200 |
    | node_network_transmit_bytes{job="node",instance="127.0.0.1", device="eth1"} | 10 100 200 |
    | node_network_transmit_bytes{job="node",instance="127.0.0.1", device="eth2"} | 10 100 200 |
    | node_network_transmit_bytes{job="node",instance="127.0.0.1", device="bond0"} | 20 200 300 |
    | node_network_transmit_bytes{job="node",instance="127.0.0.1", device="bond0"} | 20 200 300 |
    | node_bonding_slaves{job="node",instance="127.0.0.1", master="bond0"} | 2 |
  When variable `osd_hosts` is `127.0.0.1`
  Then Grafana panel `Network Load` with legend `EMPTY` shows:
    | metrics | values |
    | {} | 6 |

Scenario: "Test AVG Disk Utilization"
  Given the following series:
    | metrics | values |
    | node_disk_io_time_seconds_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_io_time_seconds_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | node_disk_io_time_seconds_total{job="node",device="sdc",instance="localhost:9100"} | 10 2000 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd_hosts` is `localhost`
  Then Grafana panel `AVG Disk Utilization` with legend `EMPTY` shows:
    | metrics | values |
    | {} | 100 |
