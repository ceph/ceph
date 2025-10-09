Feature: Host Details Dashboard

Scenario: "Test OSD"
  Given the following series:
    | metrics | values |
    | ceph_osd_metadata{job="ceph",cluster="mycluster",back_iface="",ceph_daemon="osd.0",cluster_addr="192.168.1.12",device_class="hdd",front_iface="",hostname="127.0.0.1",objectstore="bluestore",public_addr="192.168.1.12",ceph_version="ceph version 17.0.0-8967-g6932a4f702a (6932a4f702a0d557fc36df3ca7a3bca70de42667) quincy (dev)"} | 1.0 |
    | ceph_osd_metadata{job="ceph",cluster="mycluster",back_iface="",ceph_daemon="osd.1",cluster_addr="192.168.1.12",device_class="hdd",front_iface="",hostname="127.0.0.1",objectstore="bluestore",public_addr="192.168.1.12",ceph_version="ceph version 17.0.0-8967-g6932a4f702a (6932a4f702a0d557fc36df3ca7a3bca70de42667) quincy (dev)"} | 1.0 |
    | ceph_osd_metadata{job="ceph",cluster="mycluster",back_iface="",ceph_daemon="osd.2",cluster_addr="192.168.1.12",device_class="hdd",front_iface="",hostname="127.0.0.1",objectstore="bluestore",public_addr="192.168.1.12",ceph_version="ceph version 17.0.0-8967-g6932a4f702a (6932a4f702a0d557fc36df3ca7a3bca70de42667) quincy (dev)"} | 1.0 |
  When variable `ceph_hosts` is `127.0.0.1`
  Then Grafana panel `OSDs` with legend `EMPTY` shows:
    | metrics | values |
    | {}      | 3      |

# IOPS Panel - begin

Scenario: "Test Disk IOPS - Writes - Several OSDs per device"
  Given the following series:
    | metrics | values |
    | node_disk_writes_completed_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_writes_completed_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0 osd.1 osd.2",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.3 osd.4 osd.5",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Disk IOPS` with legend `{{device}}({{ceph_daemon}}) writes` shows:
    | metrics | values |
    | {job="node",ceph_daemon="osd.0 osd.1 osd.2", device="sda", instance="localhost"} | 1 |
    | {job="node",ceph_daemon="osd.3 osd.4 osd.5", device="sdb", instance="localhost"} | 1 |

Scenario: "Test Disk IOPS - Writes - Single OSD per device"
  Given the following series:
    | metrics | values |
    | node_disk_writes_completed_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_writes_completed_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Disk IOPS` with legend `{{device}}({{ceph_daemon}}) writes` shows:
    | metrics | values |
    | {job="node", ceph_daemon="osd.0", device="sda", instance="localhost"} | 1 |
    | {job="node", ceph_daemon="osd.1", device="sdb", instance="localhost"} | 1 |

Scenario: "Test Disk IOPS - Reads - Several OSDs per device"
  Given the following series:
    | metrics | values |
    | node_disk_reads_completed_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_reads_completed_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0 osd.1 osd.2",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.3 osd.4 osd.5",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Disk IOPS` with legend `{{device}}({{ceph_daemon}}) reads` shows:
    | metrics | values |
    | {job="node",ceph_daemon="osd.0 osd.1 osd.2", device="sda", instance="localhost"} | 1 |
    | {job="node",ceph_daemon="osd.3 osd.4 osd.5", device="sdb", instance="localhost"} | 1 |

Scenario: "Test Disk IOPS - Reads - Single OSD per device"
  Given the following series:
    | metrics | values |
    | node_disk_reads_completed_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_reads_completed_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Disk IOPS` with legend `{{device}}({{ceph_daemon}}) reads` shows:
    | metrics | values |
    | {job="node",ceph_daemon="osd.0", device="sda", instance="localhost"} | 1 |
    | {job="node",ceph_daemon="osd.1", device="sdb", instance="localhost"} | 1 |

# IOPS Panel - end

# Node disk bytes written/read panel - begin

Scenario: "Test disk throughput - read"
  Given the following series:
    | metrics | values |
    | node_disk_read_bytes_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_read_bytes_total{job="node",device="sdb",instance="localhost:9100"} | 100+600x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Throughput by Disk` with legend `{{device}}({{ceph_daemon}}) read` shows:
    | metrics | values |
    | {job="node",ceph_daemon="osd.0", device="sda", instance="localhost"} | 1 |
    | {job="node",ceph_daemon="osd.1", device="sdb", instance="localhost"} | 10 |

Scenario: "Test disk throughput - write"
  Given the following series:
    | metrics | values |
    | node_disk_written_bytes_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_written_bytes_total{job="node",device="sdb",instance="localhost:9100"} | 100+600x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Throughput by Disk` with legend `{{device}}({{ceph_daemon}}) write` shows:
    | metrics | values |
    | {job="node",ceph_daemon="osd.0", device="sda", instance="localhost"} | 1 |
    | {job="node",ceph_daemon="osd.1", device="sdb", instance="localhost"} | 10 |

# Node disk bytes written/read panel - end

Scenario: "Test $ceph_hosts Disk Latency panel"
  Given the following series:
    | metrics | values |
    | node_disk_write_time_seconds_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_write_time_seconds_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | node_disk_writes_completed_total{job="ndoe",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_writes_completed_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | node_disk_read_time_seconds_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_read_time_seconds_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | node_disk_reads_completed_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_reads_completed_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Disk Latency` with legend `{{device}}({{ceph_daemon}})` shows:
    | metrics | values |
    | {ceph_daemon="osd.0", device="sda", instance="localhost"} | 1 |
    | {ceph_daemon="osd.1", device="sdb", instance="localhost"} | 1 |

Scenario: "Test $ceph_hosts Disk utilization"
  Given the following series:
    | metrics | values |
    | node_disk_io_time_seconds_total{job="node",device="sda",instance="localhost:9100"} | 10+60x1 |
    | node_disk_io_time_seconds_total{job="node",device="sdb",instance="localhost:9100"} | 10+60x1 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `ceph_hosts` is `localhost`
  Then Grafana panel `$ceph_hosts Disk utilization` with legend `{{device}}({{ceph_daemon}})` shows:
    | metrics | values |
    | {job="node",ceph_daemon="osd.0", device="sda", instance="localhost"} | 100 |
    | {job="node",ceph_daemon="osd.1", device="sdb", instance="localhost"} | 100 |

