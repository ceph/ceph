Feature: OSD device details

Scenario: "Test Physical Device Latency for $osd - Reads"
  Given the following series:
    | metrics | values |
    | node_disk_reads_completed_total{device="sda",instance="localhost"} | 10 60 |
    | node_disk_reads_completed_total{device="sdb",instance="localhost"} | 10 60 |
    | node_disk_read_time_seconds_total{device="sda",instance="localhost"} | 100 600 |
    | node_disk_read_time_seconds_total{device="sdb",instance="localhost"} | 100 600 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device Latency for $osd` with legend `{{instance}}/{{device}} Reads` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 10 |

Scenario: "Test Physical Device Latency for $osd - Writes"
  Given the following series:
    | metrics | values |
    | node_disk_writes_completed_total{device="sda",instance="localhost"} | 10 60 |
    | node_disk_writes_completed_total{device="sdb",instance="localhost"} | 10 60 |
    | node_disk_write_time_seconds_total{device="sda",instance="localhost"} | 100 600 |
    | node_disk_write_time_seconds_total{device="sdb",instance="localhost"} | 100 600 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device Latency for $osd` with legend `{{instance}}/{{device}} Writes` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 10 |

Scenario: "Test Physical Device R/W IOPS for $osd - Writes"
  Given the following series:
    | metrics | values |
    | node_disk_writes_completed_total{device="sda",instance="localhost"} | 10 100 |
    | node_disk_writes_completed_total{device="sdb",instance="localhost"} | 10 100 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device R/W IOPS for $osd` with legend `{{device}} on {{instance}} Writes` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 1.5 |

Scenario: "Test Physical Device R/W IOPS for $osd - Reads"
  Given the following series:
    | metrics | values |
    | node_disk_reads_completed_total{device="sda",instance="localhost"} | 10 100 |
    | node_disk_reads_completed_total{device="sdb",instance="localhost"} | 10 100 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device R/W IOPS for $osd` with legend `{{device}} on {{instance}} Reads` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 1.5 |

Scenario: "Test Physical Device R/W Bytes for $osd - Reads"
  Given the following series:
    | metrics | values |
    | node_disk_reads_completed_total{device="sda",instance="localhost"} | 10 100 |
    | node_disk_reads_completed_total{device="sdb",instance="localhost"} | 10 100 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device R/W IOPS for $osd` with legend `{{device}} on {{instance}} Reads` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 1.5 |

Scenario: "Test Physical Device R/W Bytes for $osd - Writes"
  Given the following series:
    | metrics | values |
    | node_disk_writes_completed_total{device="sda",instance="localhost"} | 10 100 |
    | node_disk_writes_completed_total{device="sdb",instance="localhost"} | 10 100 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device R/W IOPS for $osd` with legend `{{device}} on {{instance}} Writes` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 1.5 |

Scenario: "Test Physical Device Util% for $osd"
  Given the following series:
    | metrics | values |
    | node_disk_io_time_seconds_total{device="sda",instance="localhost:9100"} | 10 100 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.0",device="/dev/sda",instance="localhost:9283"} | 1.0 |
    | ceph_disk_occupation_human{job="ceph",cluster="mycluster",ceph_daemon="osd.1",device="/dev/sdb",instance="localhost:9283"} | 1.0 |
  When variable `osd` is `osd.0`
  Then Grafana panel `Physical Device Util% for $osd` with legend `{{device}} on {{instance}}` shows:
    | metrics | values |
    | {device="sda",instance="localhost"} | 1.5 |
