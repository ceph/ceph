{
  _config:: error 'must provide _config',

  MultiClusterQuery()::
    if $._config.showMultiCluster
    then 'cluster,'
    else '',

  MultiClusterSummary()::
    if $._config.showMultiCluster
    then ' on cluster {{ $labels.cluster }}'
    else '',

  groups+: [
    {
      name: 'cluster health',
      rules: [
        {
          alert: 'CephHealthError',
          'for': '5m',
          expr: 'ceph_health_status == 2',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.2.1' },
          annotations: {
            summary: 'Ceph is in the ERROR state%(cluster)s' % $.MultiClusterSummary(),
            description: "The cluster state has been HEALTH_ERROR for more than 5 minutes%(cluster)s. Please check 'ceph health detail' for more information." % $.MultiClusterSummary(),
          },
        },
        {
          alert: 'CephHealthWarning',
          'for': '15m',
          expr: 'ceph_health_status == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'Ceph is in the WARNING state%(cluster)s' % $.MultiClusterSummary(),
            description: "The cluster state has been HEALTH_WARN for more than 15 minutes%(cluster)s. Please check 'ceph health detail' for more information." % $.MultiClusterSummary(),
          },
        },
      ],
    },
    {
      name: 'mon',
      rules: [
        {
          alert: 'CephMonDownQuorumAtRisk',
          'for': '30s',
          expr: |||
            (
              (ceph_health_detail{name="MON_DOWN"} == 1) * on() (
                count(ceph_mon_quorum_status == 1) == bool (floor(count(ceph_mon_metadata) / 2) + 1)
              )
            ) == 1
          |||,
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.3.1' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#mon-down',
            summary: 'Monitor quorum is at risk%(cluster)s' % $.MultiClusterSummary(),
            description: '{{ $min := query "floor(count(ceph_mon_metadata) / 2) + 1" | first | value }}Quorum requires a majority of monitors (x {{ $min }}) to be active. Without quorum the cluster will become inoperable, affecting all services and connected clients. The following monitors are down: {{- range query "(ceph_mon_quorum_status == 0) + on(ceph_daemon) group_left(hostname) (ceph_mon_metadata * 0)" }} - {{ .Labels.ceph_daemon }} on {{ .Labels.hostname }} {{- end }}',
          },
        },
        {
          alert: 'CephMonDown',
          'for': '30s',
          expr: |||
            count(ceph_mon_quorum_status == 0) <= (count(ceph_mon_metadata) - floor(count(ceph_mon_metadata) / 2) + 1)
          |||,
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#mon-down',
            summary: 'One or more monitors down%(cluster)s' % $.MultiClusterSummary(),
            description: |||
              {{ $down := query "count(ceph_mon_quorum_status == 0)" | first | value }}{{ $s := "" }}{{ if gt $down 1.0 }}{{ $s = "s" }}{{ end }}You have {{ $down }} monitor{{ $s }} down. Quorum is still intact, but the loss of an additional monitor will make your cluster inoperable.  The following monitors are down: {{- range query "(ceph_mon_quorum_status == 0) + on(ceph_daemon) group_left(hostname) (ceph_mon_metadata * 0)" }}   - {{ .Labels.ceph_daemon }} on {{ .Labels.hostname }} {{- end }}
            |||,
          },
        },
        {
          alert: 'CephMonDiskspaceCritical',
          'for': '1m',
          expr: 'ceph_health_detail{name="MON_DISK_CRIT"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.3.2' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#mon-disk-crit',
            summary: 'Filesystem space on at least one monitor is critically low%(cluster)s' % $.MultiClusterSummary(),
            description: "The free space available to a monitor's store is critically low. You should increase the space available to the monitor(s). The default directory is /var/lib/ceph/mon-*/data/store.db on traditional deployments, and /var/lib/rook/mon-*/data/store.db on the mon pod's worker node for Rook. Look for old, rotated versions of *.log and MANIFEST*. Do NOT touch any *.sst files. Also check any other directories under /var/lib/rook and other directories on the same filesystem, often /var/log and /var/tmp are culprits. Your monitor hosts are; {{- range query \"ceph_mon_metadata\"}} - {{ .Labels.hostname }} {{- end }}",
          },
        },
        {
          alert: 'CephMonDiskspaceLow',
          'for': '5m',
          expr: 'ceph_health_detail{name="MON_DISK_LOW"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#mon-disk-low',
            summary: 'Drive space on at least one monitor is approaching full%(cluster)s' % $.MultiClusterSummary(),
            description: "The space available to a monitor's store is approaching full (>70% is the default). You should increase the space available to the monitor(s). The default directory is /var/lib/ceph/mon-*/data/store.db on traditional deployments, and /var/lib/rook/mon-*/data/store.db on the mon pod's worker node for Rook. Look for old, rotated versions of *.log and MANIFEST*.  Do NOT touch any *.sst files. Also check any other directories under /var/lib/rook and other directories on the same filesystem, often /var/log and /var/tmp are culprits. Your monitor hosts are; {{- range query \"ceph_mon_metadata\"}} - {{ .Labels.hostname }} {{- end }}",
          },
        },
        {
          alert: 'CephMonClockSkew',
          'for': '1m',
          expr: 'ceph_health_detail{name="MON_CLOCK_SKEW"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#mon-clock-skew',
            summary: 'Clock skew detected among monitors%(cluster)s' % $.MultiClusterSummary(),
            description: "Ceph monitors rely on closely synchronized time to maintain quorum and cluster consistency. This event indicates that the time on at least one mon has drifted too far from the lead mon. Review cluster status with ceph -s. This will show which monitors are affected. Check the time sync status on each monitor host with 'ceph time-sync-status' and the state and peers of your ntpd or chrony daemon.",
          },
        },
      ],
    },
    {
      name: 'osd',
      rules: [
        {
          alert: 'CephOSDDownHigh',
          expr: 'count(ceph_osd_up == 0) / count(ceph_osd_up) * 100 >= 10',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.1' },
          annotations: {
            summary: 'More than 10%% of OSDs are down%(cluster)s' % $.MultiClusterSummary(),
            description: '{{ $value | humanize }}% or {{ with query "count(ceph_osd_up == 0)" }}{{ . | first | value }}{{ end }} of {{ with query "count(ceph_osd_up)" }}{{ . | first | value }}{{ end }} OSDs are down (>= 10%). The following OSDs are down: {{- range query "(ceph_osd_up * on(ceph_daemon) group_left(hostname) ceph_osd_metadata) == 0" }} - {{ .Labels.ceph_daemon }} on {{ .Labels.hostname }} {{- end }}',
          },
        },
        {
          alert: 'CephOSDHostDown',
          'for': '5m',
          expr: 'ceph_health_detail{name="OSD_HOST_DOWN"} == 1',
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.8' },
          annotations: {
            summary: 'An OSD host is offline%(cluster)s' % $.MultiClusterSummary(),
            description: 'The following OSDs are down: {{- range query "(ceph_osd_up * on(ceph_daemon) group_left(hostname) ceph_osd_metadata) == 0" }} - {{ .Labels.hostname }} : {{ .Labels.ceph_daemon }} {{- end }}',
          },
        },
        {
          alert: 'CephOSDDown',
          'for': '5m',
          expr: 'ceph_health_detail{name="OSD_DOWN"} == 1',
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.2' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#osd-down',
            summary: 'An OSD has been marked down%(cluster)s' % $.MultiClusterSummary(),
            description: |||
              {{ $num := query "count(ceph_osd_up == 0)" | first | value }}{{ $s := "" }}{{ if gt $num 1.0 }}{{ $s = "s" }}{{ end }}{{ $num }} OSD{{ $s }} down for over 5mins. The following OSD{{ $s }} {{ if eq $s "" }}is{{ else }}are{{ end }} down: {{- range query "(ceph_osd_up * on(ceph_daemon) group_left(hostname) ceph_osd_metadata) == 0"}} - {{ .Labels.ceph_daemon }} on {{ .Labels.hostname }} {{- end }}
            |||,
          },
        },
        {
          alert: 'CephOSDNearFull',
          'for': '5m',
          expr: 'ceph_health_detail{name="OSD_NEARFULL"} == 1',
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.3' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#osd-nearfull',
            summary: 'OSD(s) running low on free space (NEARFULL)%(cluster)s' % $.MultiClusterSummary(),
            description: "One or more OSDs have reached the NEARFULL threshold. Use 'ceph health detail' and 'ceph osd df' to identify the problem. To resolve, add capacity to the affected OSD's failure domain, restore down/out OSDs, or delete unwanted data.",
          },
        },
        {
          alert: 'CephOSDFull',
          'for': '1m',
          expr: 'ceph_health_detail{name="OSD_FULL"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.6' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#osd-full',
            summary: 'OSD full, writes blocked%(cluster)s' % $.MultiClusterSummary(),
            description: "An OSD has reached the FULL threshold. Writes to pools that share the affected OSD will be blocked. Use 'ceph health detail' and 'ceph osd df' to identify the problem. To resolve, add capacity to the affected OSD's failure domain, restore down/out OSDs, or delete unwanted data.",
          },
        },
        {
          alert: 'CephOSDBackfillFull',
          'for': '1m',
          expr: 'ceph_health_detail{name="OSD_BACKFILLFULL"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#osd-backfillfull',
            summary: 'OSD(s) too full for backfill operations%(cluster)s' % $.MultiClusterSummary(),
            description: "An OSD has reached the BACKFILL FULL threshold. This will prevent rebalance operations from completing. Use 'ceph health detail' and 'ceph osd df' to identify the problem. To resolve, add capacity to the affected OSD's failure domain, restore down/out OSDs, or delete unwanted data.",
          },
        },
        {
          alert: 'CephOSDTooManyRepairs',
          'for': '30s',
          expr: 'ceph_health_detail{name="OSD_TOO_MANY_REPAIRS"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#osd-too-many-repairs',
            summary: 'OSD reports a high number of read errors%(cluster)s' % $.MultiClusterSummary(),
            description: 'Reads from an OSD have used a secondary PG to return data to the client, indicating a potential failing drive.',
          },
        },
        {
          alert: 'CephOSDTimeoutsPublicNetwork',
          'for': '1m',
          expr: 'ceph_health_detail{name="OSD_SLOW_PING_TIME_FRONT"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'Network issues delaying OSD heartbeats (public network)%(cluster)s' % $.MultiClusterSummary(),
            description: "OSD heartbeats on the cluster's 'public' network (frontend) are running slow. Investigate the network for latency or loss issues. Use 'ceph health detail' to show the affected OSDs.",
          },
        },
        {
          alert: 'CephOSDTimeoutsClusterNetwork',
          'for': '1m',
          expr: 'ceph_health_detail{name="OSD_SLOW_PING_TIME_BACK"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'Network issues delaying OSD heartbeats (cluster network)%(cluster)s' % $.MultiClusterSummary(),
            description: "OSD heartbeats on the cluster's 'cluster' network (backend) are slow. Investigate the network for latency issues on this subnet. Use 'ceph health detail' to show the affected OSDs.",
          },
        },
        {
          alert: 'CephOSDInternalDiskSizeMismatch',
          'for': '1m',
          expr: 'ceph_health_detail{name="BLUESTORE_DISK_SIZE_MISMATCH"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#bluestore-disk-size-mismatch',
            summary: 'OSD size inconsistency error%(cluster)s' % $.MultiClusterSummary(),
            description: 'One or more OSDs have an internal inconsistency between metadata and the size of the device. This could lead to the OSD(s) crashing in future. You should redeploy the affected OSDs.',
          },
        },
        {
          alert: 'CephDeviceFailurePredicted',
          'for': '1m',
          expr: 'ceph_health_detail{name="DEVICE_HEALTH"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#id2',
            summary: 'Device(s) predicted to fail soon%(cluster)s' % $.MultiClusterSummary(),
            description: "The device health module has determined that one or more devices will fail soon. To review device status use 'ceph device ls'. To show a specific device use 'ceph device info <dev id>'. Mark the OSD out so that data may migrate to other OSDs. Once the OSD has drained, destroy the OSD, replace the device, and redeploy the OSD.",
          },
        },
        {
          alert: 'CephDeviceFailurePredictionTooHigh',
          'for': '1m',
          expr: 'ceph_health_detail{name="DEVICE_HEALTH_TOOMANY"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.7' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#device-health-toomany',
            summary: 'Too many devices are predicted to fail, unable to resolve%(cluster)s' % $.MultiClusterSummary(),
            description: 'The device health module has determined that devices predicted to fail can not be remediated automatically, since too many OSDs would be removed from the cluster to ensure performance and availabililty. Prevent data integrity issues by adding new OSDs so that data may be relocated.',
          },
        },
        {
          alert: 'CephDeviceFailureRelocationIncomplete',
          'for': '1m',
          expr: 'ceph_health_detail{name="DEVICE_HEALTH_IN_USE"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#device-health-in-use',
            summary: 'Device failure is predicted, but unable to relocate data%(cluster)s' % $.MultiClusterSummary(),
            description: 'The device health module has determined that one or more devices will fail soon, but the normal process of relocating the data on the device to other OSDs in the cluster is blocked. \nEnsure that the cluster has available free space. It may be necessary to add capacity to the cluster to allow data from the failing device to successfully migrate, or to enable the balancer.',
          },
        },
        {
          alert: 'CephOSDFlapping',
          expr: '(rate(ceph_osd_up[5m]) * on(%(cluster)sceph_daemon) group_left(hostname) ceph_osd_metadata) * 60 > 1' % $.MultiClusterQuery(),
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.4' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/troubleshooting/troubleshooting-osd#flapping-osds',
            summary: 'Network issues are causing OSDs to flap (mark each other down)%(cluster)s' % $.MultiClusterSummary(),
            description: 'OSD {{ $labels.ceph_daemon }} on {{ $labels.hostname }} was marked down and back up {{ $value | humanize }} times once a minute for 5 minutes. This may indicate a network issue (latency, packet loss, MTU mismatch) on the cluster network, or the public network if no cluster network is deployed. Check the network stats on the listed host(s).',
          },
        },
        {
          alert: 'CephOSDReadErrors',
          'for': '30s',
          expr: 'ceph_health_detail{name="BLUESTORE_SPURIOUS_READ_ERRORS"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#bluestore-spurious-read-errors',
            summary: 'Device read errors detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'An OSD has encountered read errors, but the OSD has recovered by retrying the reads. This may indicate an issue with hardware or the kernel.',
          },
        },
        {
          alert: 'CephPGImbalance',
          'for': '5m',
          expr: |||
            abs(
              ((ceph_osd_numpg > 0) - on (%(cluster)sjob) group_left avg(ceph_osd_numpg > 0) by (%(cluster)sjob)) /
              on (job) group_left avg(ceph_osd_numpg > 0) by (job)
            ) * on (%(cluster)sceph_daemon) group_left(hostname) ceph_osd_metadata > 0.30
          ||| % [$.MultiClusterQuery(), $.MultiClusterQuery(), $.MultiClusterQuery()],
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.4.5' },
          annotations: {
            summary: 'PGs are not balanced across OSDs%(cluster)s' % $.MultiClusterSummary(),
            description: 'OSD {{ $labels.ceph_daemon }} on {{ $labels.hostname }} deviates by more than 30% from average PG count.',
          },
        },
      ],
    },
    {
      name: 'mds',
      rules: [
        {
          alert: 'CephFilesystemDamaged',
          'for': '1m',
          expr: 'ceph_health_detail{name="MDS_DAMAGE"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.5.1' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages#cephfs-health-messages',
            summary: 'CephFS filesystem is damaged%(cluster)s.' % $.MultiClusterSummary(),
            description: 'Filesystem metadata has been corrupted. Data may be inaccessible. Analyze metrics from the MDS daemon admin socket, or escalate to support.',
          },
        },
        {
          alert: 'CephFilesystemOffline',
          'for': '1m',
          expr: 'ceph_health_detail{name="MDS_ALL_DOWN"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.5.3' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages/#mds-all-down',
            summary: 'CephFS filesystem is offline%(cluster)s' % $.MultiClusterSummary(),
            description: 'All MDS ranks are unavailable. The MDS daemons managing metadata are down, rendering the filesystem offline.',
          },
        },
        {
          alert: 'CephFilesystemDegraded',
          'for': '1m',
          expr: 'ceph_health_detail{name="FS_DEGRADED"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.5.4' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages/#fs-degraded',
            summary: 'CephFS filesystem is degraded%(cluster)s' % $.MultiClusterSummary(),
            description: 'One or more metadata daemons (MDS ranks) are failed or in a damaged state. At best the filesystem is partially available, at worst the filesystem is completely unusable.',
          },
        },
        {
          alert: 'CephFilesystemMDSRanksLow',
          'for': '1m',
          expr: 'ceph_health_detail{name="MDS_UP_LESS_THAN_MAX"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages/#mds-up-less-than-max',
            summary: 'Ceph MDS daemon count is lower than configured%(cluster)s' % $.MultiClusterSummary(),
            description: "The filesystem's 'max_mds' setting defines the number of MDS ranks in the filesystem. The current number of active MDS daemons is less than this value.",
          },
        },
        {
          alert: 'CephFilesystemInsufficientStandby',
          'for': '1m',
          expr: 'ceph_health_detail{name="MDS_INSUFFICIENT_STANDBY"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages/#mds-insufficient-standby',
            summary: 'Ceph filesystem standby daemons too few%(cluster)s' % $.MultiClusterSummary(),
            description: 'The minimum number of standby daemons required by standby_count_wanted is less than the current number of standby daemons. Adjust the standby count or increase the number of MDS daemons.',
          },
        },
        {
          alert: 'CephFilesystemFailureNoStandby',
          'for': '1m',
          expr: 'ceph_health_detail{name="FS_WITH_FAILED_MDS"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.5.5' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages/#fs-with-failed-mds',
            summary: 'MDS daemon failed, no further standby available%(cluster)s' % $.MultiClusterSummary(),
            description: 'An MDS daemon has failed, leaving only one active rank and no available standby. Investigate the cause of the failure or add a standby MDS.',
          },
        },
        {
          alert: 'CephFilesystemReadOnly',
          'for': '1m',
          expr: 'ceph_health_detail{name="MDS_HEALTH_READ_ONLY"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.5.2' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephfs/health-messages#cephfs-health-messages',
            summary: 'CephFS filesystem in read only mode due to write error(s)%(cluster)s' % $.MultiClusterSummary(),
            description: 'The filesystem has switched to READ ONLY due to an unexpected error when writing to the metadata pool. Either analyze the output from the MDS daemon admin socket, or escalate to support.',
          },
        },
      ],
    },
    {
      name: 'mgr',
      rules: [
        {
          alert: 'CephMgrModuleCrash',
          'for': '5m',
          expr: 'ceph_health_detail{name="RECENT_MGR_MODULE_CRASH"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.6.1' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#recent-mgr-module-crash',
            summary: 'A manager module has recently crashed%(cluster)s' % $.MultiClusterSummary(),
            description: "One or more mgr modules have crashed and have yet to be acknowledged by an administrator. A crashed module may impact functionality within the cluster. Use the 'ceph crash' command to determine which module has failed, and archive it to acknowledge the failure.",
          },
        },
        {
          alert: 'CephMgrPrometheusModuleInactive',
          'for': '1m',
          expr: 'up{job="ceph"} == 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.6.2' },
          annotations: {
            summary: 'The mgr/prometheus module is not available%(cluster)s' % $.MultiClusterSummary(),
            description: "The mgr/prometheus module at {{ $labels.instance }} is unreachable. This could mean that the module has been disabled or the mgr daemon itself is down. Without the mgr/prometheus module metrics and alerts will no longer function. Open a shell to an admin node or toolbox pod and use 'ceph -s' to to determine whether the mgr is active. If the mgr is not active, restart it, otherwise you can determine module status with 'ceph mgr module ls'. If it is not listed as enabled, enable it with 'ceph mgr module enable prometheus'.",
          },
        },
      ],
    },
    {
      name: 'pgs',
      rules: [
        {
          alert: 'CephPGsInactive',
          'for': '5m',
          expr: 'ceph_pool_metadata * on(%(cluster)spool_id,instance) group_left() (ceph_pg_total - ceph_pg_active) > 0' % $.MultiClusterQuery(),
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.7.1' },
          annotations: {
            summary: 'One or more placement groups are inactive%(cluster)s' % $.MultiClusterSummary(),
            description: '{{ $value }} PGs have been inactive for more than 5 minutes in pool {{ $labels.name }}. Inactive placement groups are not able to serve read/write requests.',
          },
        },
        {
          alert: 'CephPGsUnclean',
          'for': '15m',
          expr: 'ceph_pool_metadata * on(%(cluster)spool_id,instance) group_left() (ceph_pg_total - ceph_pg_clean) > 0' % $.MultiClusterQuery(),
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.7.2' },
          annotations: {
            summary: 'One or more placement groups are marked unclean%(cluster)s' % $.MultiClusterSummary(),
            description: '{{ $value }} PGs have been unclean for more than 15 minutes in pool {{ $labels.name }}. Unclean PGs have not recovered from a previous failure.',
          },
        },
        {
          alert: 'CephPGsDamaged',
          'for': '5m',
          expr: 'ceph_health_detail{name=~"PG_DAMAGED|OSD_SCRUB_ERRORS"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.7.4' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pg-damaged',
            summary: 'Placement group damaged, manual intervention needed%(cluster)s' % $.MultiClusterSummary(),
            description: "During data consistency checks (scrub), at least one PG has been flagged as being damaged or inconsistent. Check to see which PG is affected, and attempt a manual repair if necessary. To list problematic placement groups, use 'rados list-inconsistent-pg <pool>'. To repair PGs use the 'ceph pg repair <pg_num>' command.",
          },
        },
        {
          alert: 'CephPGRecoveryAtRisk',
          'for': '1m',
          expr: 'ceph_health_detail{name="PG_RECOVERY_FULL"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.7.5' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pg-recovery-full',
            summary: 'OSDs are too full for recovery%(cluster)s' % $.MultiClusterSummary(),
            description: "Data redundancy is at risk since one or more OSDs are at or above the 'full' threshold. Add more capacity to the cluster, restore down/out OSDs, or delete unwanted data.",
          },
        },
        {
          alert: 'CephPGUnavilableBlockingIO',
          'for': '1m',
          expr: '((ceph_health_detail{name="PG_AVAILABILITY"} == 1) - scalar(ceph_health_detail{name="OSD_DOWN"})) == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.7.3' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pg-availability',
            summary: 'PG is unavailable%(cluster)s, blocking I/O' % $.MultiClusterSummary(),
            description: "Data availability is reduced, impacting the cluster's ability to service I/O. One or more placement groups (PGs) are in a state that blocks I/O.",
          },
        },
        {
          alert: 'CephPGBackfillAtRisk',
          'for': '1m',
          expr: 'ceph_health_detail{name="PG_BACKFILL_FULL"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.7.6' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pg-backfill-full',
            summary: 'Backfill operations are blocked due to lack of free space%(cluster)s' % $.MultiClusterSummary(),
            description: "Data redundancy may be at risk due to lack of free space within the cluster. One or more OSDs have reached the 'backfillfull' threshold. Add more capacity, or delete unwanted data.",
          },
        },
        {
          alert: 'CephPGNotScrubbed',
          'for': '5m',
          expr: 'ceph_health_detail{name="PG_NOT_SCRUBBED"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pg-not-scrubbed',
            summary: 'Placement group(s) have not been scrubbed%(cluster)s' % $.MultiClusterSummary(),
            description: "One or more PGs have not been scrubbed recently. Scrubs check metadata integrity, protecting against bit-rot. They check that metadata is consistent across data replicas. When PGs miss their scrub interval, it may indicate that the scrub window is too small, or PGs were not in a 'clean' state during the scrub window. You can manually initiate a scrub with: ceph pg scrub <pgid>",
          },
        },
        {
          alert: 'CephPGsHighPerOSD',
          'for': '1m',
          expr: 'ceph_health_detail{name="TOO_MANY_PGS"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks/#too-many-pgs',
            summary: 'Placement groups per OSD is too high%(cluster)s' % $.MultiClusterSummary(),
            description: "The number of placement groups per OSD is too high (exceeds the mon_max_pg_per_osd setting).\n Check that the pg_autoscaler has not been disabled for any pools with 'ceph osd pool autoscale-status', and that the profile selected is appropriate. You may also adjust the target_size_ratio of a pool to guide the autoscaler based on the expected relative size of the pool ('ceph osd pool set cephfs.cephfs.meta target_size_ratio .1') or set the pg_autoscaler mode to 'warn' and adjust pg_num appropriately for one or more pools.",
          },
        },
        {
          alert: 'CephPGNotDeepScrubbed',
          'for': '5m',
          expr: 'ceph_health_detail{name="PG_NOT_DEEP_SCRUBBED"} == 1',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pg-not-deep-scrubbed',
            summary: 'Placement group(s) have not been deep scrubbed%(cluster)s' % $.MultiClusterSummary(),
            description: "One or more PGs have not been deep scrubbed recently. Deep scrubs protect against bit-rot. They compare data replicas to ensure consistency. When PGs miss their deep scrub interval, it may indicate that the window is too small or PGs were not in a 'clean' state during the deep-scrub window.",
          },
        },
      ],
    },
    {
      name: 'nodes',
      rules: [
        {
          alert: 'CephNodeRootFilesystemFull',
          'for': '5m',
          expr: 'node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"} * 100 < 5',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.8.1' },
          annotations: {
            summary: 'Root filesystem is dangerously full%(cluster)s' % $.MultiClusterSummary(),
            description: 'Root volume is dangerously full: {{ $value | humanize }}% free.',
          },
        },
        {
          alert: 'CephNodeNetworkPacketDrops',
          expr: |||
            (
              rate(node_network_receive_drop_total{device!="lo"}[1m]) +
              rate(node_network_transmit_drop_total{device!="lo"}[1m])
            ) / (
              rate(node_network_receive_packets_total{device!="lo"}[1m]) +
              rate(node_network_transmit_packets_total{device!="lo"}[1m])
            ) >= %(CephNodeNetworkPacketDropsThreshold)s and (
              rate(node_network_receive_drop_total{device!="lo"}[1m]) +
              rate(node_network_transmit_drop_total{device!="lo"}[1m])
            ) >= %(CephNodeNetworkPacketDropsPerSec)s
          ||| % $._config,
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.8.2' },
          annotations: {
            summary: 'One or more NICs reports packet drops%(cluster)s' % $.MultiClusterSummary(),
            description: 'Node {{ $labels.instance }} experiences packet drop > %(CephNodeNetworkPacketDropsThreshold)s%% or > %(CephNodeNetworkPacketDropsPerSec)s packets/s on interface {{ $labels.device }}.' % { CephNodeNetworkPacketDropsThreshold: $._config.CephNodeNetworkPacketDropsThreshold * 100, CephNodeNetworkPacketDropsPerSec: $._config.CephNodeNetworkPacketDropsPerSec },
          },
        },
        {
          alert: 'CephNodeNetworkPacketErrors',
          expr: |||
            (
              rate(node_network_receive_errs_total{device!="lo"}[1m]) +
              rate(node_network_transmit_errs_total{device!="lo"}[1m])
            ) / (
              rate(node_network_receive_packets_total{device!="lo"}[1m]) +
              rate(node_network_transmit_packets_total{device!="lo"}[1m])
            ) >= 0.0001 or (
              rate(node_network_receive_errs_total{device!="lo"}[1m]) +
              rate(node_network_transmit_errs_total{device!="lo"}[1m])
            ) >= 10
          |||,
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.8.3' },
          annotations: {
            summary: 'One or more NICs reports packet errors%(cluster)s' % $.MultiClusterSummary(),
            description: 'Node {{ $labels.instance }} experiences packet errors > 0.01% or > 10 packets/s on interface {{ $labels.device }}.',
          },
        },
        {
          alert: 'CephNodeNetworkBondDegraded',
          expr: |||
            node_bonding_slaves - node_bonding_active != 0
          |||,
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'Degraded Bond on Node {{ $labels.instance }}%(cluster)s' % $.MultiClusterSummary(),
            description: 'Bond {{ $labels.master }} is degraded on Node {{ $labels.instance }}.',
          },
        },
        {
          alert: 'CephNodeDiskspaceWarning',
          expr: 'predict_linear(node_filesystem_free_bytes{device=~"/.*"}[2d], 3600 * 24 * 5) *on(instance) group_left(nodename) node_uname_info < 0',
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.8.4' },
          annotations: {
            summary: 'Host filesystem free space is getting low%(cluster)s' % $.MultiClusterSummary(),
            description: 'Mountpoint {{ $labels.mountpoint }} on {{ $labels.nodename }} will be full in less than 5 days based on the 48 hour trailing fill rate.',
          },
        },
        {
          alert: 'CephNodeInconsistentMTU',
          expr: 'node_network_mtu_bytes * (node_network_up{device!="lo"} > 0) ==  scalar(    max by (device) (node_network_mtu_bytes * (node_network_up{device!="lo"} > 0)) !=      quantile by (device) (.5, node_network_mtu_bytes * (node_network_up{device!="lo"} > 0))  )or node_network_mtu_bytes * (node_network_up{device!="lo"} > 0) ==  scalar(    min by (device) (node_network_mtu_bytes * (node_network_up{device!="lo"} > 0)) !=      quantile by (device) (.5, node_network_mtu_bytes * (node_network_up{device!="lo"} > 0))  )',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'MTU settings across Ceph hosts are inconsistent%(cluster)s' % $.MultiClusterSummary(),
            description: 'Node {{ $labels.instance }} has a different MTU size ({{ $value }}) than the median of devices named {{ $labels.device }}.',
          },
        },
      ],
    },
    {
      name: 'pools',
      rules: [
        {
          alert: 'CephPoolGrowthWarning',
          expr: '(predict_linear(ceph_pool_percent_used[2d], 3600 * 24 * 5) * on(%(cluster)spool_id)    group_right ceph_pool_metadata) >= 95' % $.MultiClusterQuery(),
          labels: { severity: 'warning', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.9.2' },
          annotations: {
            summary: 'Pool growth rate may soon exceed capacity%(cluster)s' % $.MultiClusterSummary(),
            description: "Pool '{{ $labels.name }}' will be full in less than 5 days assuming the average fill-up rate of the past 48 hours.",
          },
        },
        {
          alert: 'CephPoolBackfillFull',
          expr: 'ceph_health_detail{name="POOL_BACKFILLFULL"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'Free space in a pool is too low for recovery/backfill%(cluster)s' % $.MultiClusterSummary(),
            description: 'A pool is approaching the near full threshold, which will prevent recovery/backfill operations from completing. Consider adding more capacity.',
          },
        },
        {
          alert: 'CephPoolFull',
          'for': '1m',
          expr: 'ceph_health_detail{name="POOL_FULL"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.9.1' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#pool-full',
            summary: 'Pool is full - writes are blocked%(cluster)s' % $.MultiClusterSummary(),
            description: "A pool has reached its MAX quota, or OSDs supporting the pool have reached the FULL threshold. Until this is resolved, writes to the pool will be blocked. Pool Breakdown (top 5) {{- range query \"topk(5, sort_desc(ceph_pool_percent_used * on(pool_id) group_right ceph_pool_metadata))\" }} - {{ .Labels.name }} at {{ .Value }}% {{- end }} Increase the pool's quota, or add capacity to the cluster first then increase the pool's quota (e.g. ceph osd pool set quota <pool_name> max_bytes <bytes>)",
          },
        },
        {
          alert: 'CephPoolNearFull',
          'for': '5m',
          expr: 'ceph_health_detail{name="POOL_NEAR_FULL"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            summary: 'One or more Ceph pools are nearly full%(cluster)s' % $.MultiClusterSummary(),
            description: "A pool has exceeded the warning (percent full) threshold, or OSDs supporting the pool have reached the NEARFULL threshold. Writes may continue, but you are at risk of the pool going read-only if more capacity isn't made available. Determine the affected pool with 'ceph df detail', looking at QUOTA BYTES and STORED. Increase the pool's quota, or add capacity to the cluster first then increase the pool's quota (e.g. ceph osd pool set quota <pool_name> max_bytes <bytes>). Also ensure that the balancer is active.",
          },
        },
      ],
    },
    {
      name: 'healthchecks',
      rules: [
        {
          alert: 'CephSlowOps',
          'for': '30s',
          expr: 'ceph_healthcheck_slow_ops > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#slow-ops',
            summary: 'OSD operations are slow to complete%(cluster)s' % $.MultiClusterSummary(),
            description: '{{ $value }} OSD requests are taking too long to process (osd_op_complaint_time exceeded)',
          },
        },
        {
          alert: 'CephDaemonSlowOps',
          'for': '30s',
          expr: 'ceph_daemon_health_metrics{type="SLOW_OPS"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#slow-ops',
            summary: '{{ $labels.ceph_daemon }} operations are slow to complete',
            description: '{{ $labels.ceph_daemon }} operations are taking too long to process (complaint time exceeded)',
          },
        },
      ],
    },
    {
      name: 'cephadm',
      rules: [
        {
          alert: 'CephadmUpgradeFailed',
          'for': '30s',
          expr: 'ceph_health_detail{name="UPGRADE_EXCEPTION"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.11.2' },
          annotations: {
            summary: 'Ceph version upgrade has failed%(cluster)s' % $.MultiClusterSummary(),
            description: 'The cephadm cluster upgrade process has failed. The cluster remains in an undetermined state. Please review the cephadm logs, to understand the nature of the issue',
          },
        },
        {
          alert: 'CephadmDaemonFailed',
          'for': '30s',
          expr: 'ceph_health_detail{name="CEPHADM_FAILED_DAEMON"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.11.1' },
          annotations: {
            summary: 'A ceph daemon manged by cephadm is down%(cluster)s' % $.MultiClusterSummary(),
            description: "A daemon managed by cephadm is no longer active. Determine, which daemon is down with 'ceph health detail'. you may start daemons with the 'ceph orch daemon start <daemon_id>'",
          },
        },
        {
          alert: 'CephadmPaused',
          'for': '1m',
          expr: 'ceph_health_detail{name="CEPHADM_PAUSED"} > 0',
          labels: { severity: 'warning', type: 'ceph_default' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/cephadm/operations#cephadm-paused',
            summary: 'Orchestration tasks via cephadm are PAUSED%(cluster)s' % $.MultiClusterSummary(),
            description: "Cluster management has been paused manually. This will prevent the orchestrator from service management and reconciliation. If this is not intentional, resume cephadm operations with 'ceph orch resume'",
          },
        },
      ],
    },
    {
      name: 'hardware',
      rules: [
        {
          alert: 'HardwareStorageError',
          'for': '30s',
          expr: 'ceph_health_detail{name="HARDWARE_STORAGE"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.13.1' },
          annotations: {
            summary: 'Storage devices error(s) detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'Some storage devices are in error. Check `ceph health detail`.',
          },
        },
        {
          alert: 'HardwareMemoryError',
          'for': '30s',
          expr: 'ceph_health_detail{name="HARDWARE_MEMORY"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.13.2' },
          annotations: {
            summary: 'DIMM error(s) detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'DIMM error(s) detected. Check `ceph health detail`.',
          },
        },
        {
          alert: 'HardwareProcessorError',
          'for': '30s',
          expr: 'ceph_health_detail{name="HARDWARE_PROCESSOR"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.13.3' },
          annotations: {
            summary: 'Processor error(s) detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'Processor error(s) detected. Check `ceph health detail`.',
          },
        },
        {
          alert: 'HardwareNetworkError',
          'for': '30s',
          expr: 'ceph_health_detail{name="HARDWARE_NETWORK"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.13.4' },
          annotations: {
            summary: 'Network error(s) detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'Network error(s) detected. Check `ceph health detail`.',
          },
        },
        {
          alert: 'HardwarePowerError',
          'for': '30s',
          expr: 'ceph_health_detail{name="HARDWARE_POWER"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.13.5' },
          annotations: {
            summary: 'Power supply error(s) detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'Power supply error(s) detected. Check `ceph health detail`.',
          },
        },
        {
          alert: 'HardwareFanError',
          'for': '30s',
          expr: 'ceph_health_detail{name="HARDWARE_FANS"} > 0',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.13.6' },
          annotations: {
            summary: 'Fan error(s) detected%(cluster)s' % $.MultiClusterSummary(),
            description: 'Fan error(s) detected. Check `ceph health detail`.',
          },
        },
      ],
    },
    {
      name: 'PrometheusServer',
      rules: [
        {
          alert: 'PrometheusJobMissing',
          'for': '30s',
          expr: 'absent(up{job="ceph"})',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.12.1' },
          annotations: {
            summary: 'The scrape job for Ceph is missing from Prometheus%(cluster)s' % $.MultiClusterSummary(),
            description: "The prometheus job that scrapes from Ceph is no longer defined, this will effectively mean you'll have no metrics or alerts for the cluster.  Please review the job definitions in the prometheus.yml file of the prometheus instance.",
          },
        },
      ],
    },
    {
      name: 'rados',
      rules: [
        {
          alert: 'CephObjectMissing',
          'for': '30s',
          expr: '(ceph_health_detail{name="OBJECT_UNFOUND"} == 1) * on() (count(ceph_osd_up == 1) == bool count(ceph_osd_metadata)) == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.10.1' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks#object-unfound',
            summary: 'Object(s) marked UNFOUND%(cluster)s' % $.MultiClusterSummary(),
            description: 'The latest version of a RADOS object can not be found, even though all OSDs are up. I/O requests for this object from clients will block (hang). Resolving this issue may require the object to be rolled back to a prior version manually, and manually verified.',
          },
        },
      ],
    },
    {
      name: 'generic',
      rules: [
        {
          alert: 'CephDaemonCrash',
          'for': '1m',
          expr: 'ceph_health_detail{name="RECENT_CRASH"} == 1',
          labels: { severity: 'critical', type: 'ceph_default', oid: '1.3.6.1.4.1.50495.1.2.1.1.2' },
          annotations: {
            documentation: 'https://docs.ceph.com/en/latest/rados/operations/health-checks/#recent-crash',
            summary: 'One or more Ceph daemons have crashed, and are pending acknowledgement%(cluster)s' % $.MultiClusterSummary(),
            description: "One or more daemons have crashed recently, and need to be acknowledged. This notification ensures that software crashes do not go unseen. To acknowledge a crash, use the 'ceph crash archive <id>' command.",
          },
        },
      ],
    },
  ],
}
