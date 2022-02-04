local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';

{
  grafanaDashboards+:: {
    'hosts-overview.json':
      local HostsOverviewSingleStatPanel(format,
                                         title,
                                         description,
                                         valueName,
                                         expr,
                                         targetFormat,
                                         x,
                                         y,
                                         w,
                                         h) =
        u.addSingleStatSchema(['#299c46', 'rgba(237, 129, 40, 0.89)', '#d44a3a'],
                              '$datasource',
                              format,
                              title,
                              description,
                              valueName,
                              false,
                              100,
                              false,
                              false,
                              '')
        .addTarget(
          u.addTargetSchema(expr, 1, targetFormat, '')
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      local HostsOverviewGraphPanel(title, description, formatY1, expr, legendFormat, x, y, w, h) =
        u.graphPanelSchema(
          {}, title, description, 'null', false, formatY1, 'short', null, null, 0, 1, '$datasource'
        )
        .addTargets(
          [u.addTargetSchema(
            expr, 1, 'time_series', legendFormat
          )]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'Host Overview',
        '',
        'y0KGL0iZz',
        'now-1h',
        '10s',
        16,
        [],
        '',
        {
          refresh_intervals: ['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
      )
      .addRequired(
        type='grafana', id='grafana', name='Grafana', version='5.3.2'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addRequired(
        type='panel', id='singlestat', name='Singlestat', version='5.0.0'
      )
      .addAnnotation(
        u.addAnnotationSchema(
          1,
          '-- Grafana --',
          true,
          true,
          'rgba(0, 211, 255, 1)',
          'Annotations & Alerts',
          'dashboard'
        )
      )
      .addTemplate(
        g.template.datasource('datasource',
                              'prometheus',
                              'default',
                              label='Data Source')
      )
      .addTemplate(
        u.addTemplateSchema('osd_hosts',
                            '$datasource',
                            'label_values(ceph_disk_occupation, exported_instance)',
                            1,
                            true,
                            1,
                            null,
                            '([^.]*).*')
      )
      .addTemplate(
        u.addTemplateSchema('mon_hosts',
                            '$datasource',
                            'label_values(ceph_mon_metadata, ceph_daemon)',
                            1,
                            true,
                            1,
                            null,
                            'mon.(.*)')
      )
      .addTemplate(
        u.addTemplateSchema('mds_hosts',
                            '$datasource',
                            'label_values(ceph_mds_inodes, ceph_daemon)',
                            1,
                            true,
                            1,
                            null,
                            'mds.(.*)')
      )
      .addTemplate(
        u.addTemplateSchema('rgw_hosts',
                            '$datasource',
                            'label_values(ceph_rgw_metadata, ceph_daemon)',
                            1,
                            true,
                            1,
                            null,
                            'rgw.(.*)')
      )
      .addPanels([
        HostsOverviewSingleStatPanel(
          'none',
          'OSD Hosts',
          '',
          'current',
          'count(sum by (hostname) (ceph_osd_metadata))',
          'time_series',
          0,
          0,
          4,
          5
        ),
        HostsOverviewSingleStatPanel(
          'percentunit',
          'AVG CPU Busy',
          'Average CPU busy across all hosts (OSD, RGW, MON etc) within the cluster',
          'current',
          'avg(\n  1 - (\n    avg by(instance) \n      (irate(node_cpu_seconds_total{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]) or\n       irate(node_cpu{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]))\n    )\n  )',
          'time_series',
          4,
          0,
          4,
          5
        ),
        HostsOverviewSingleStatPanel(
          'percentunit',
          'AVG RAM Utilization',
          'Average Memory Usage across all hosts in the cluster (excludes buffer/cache usage)',
          'current',
          'avg (((node_memory_MemTotal{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or node_memory_MemTotal_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"})- (\n  (node_memory_MemFree{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or node_memory_MemFree_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"})  + \n  (node_memory_Cached{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or node_memory_Cached_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}) + \n  (node_memory_Buffers{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or node_memory_Buffers_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}) +\n  (node_memory_Slab{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or node_memory_Slab_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"})\n  )) /\n (node_memory_MemTotal{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or node_memory_MemTotal_bytes{instance=~"($osd_hosts|$rgw_hosts|$mon_hosts|$mds_hosts).*"} ))',
          'time_series',
          8,
          0,
          4,
          5
        ),
        HostsOverviewSingleStatPanel(
          'none',
          'Physical IOPS',
          'IOPS Load at the device as reported by the OS on all OSD hosts',
          'current',
          'sum ((irate(node_disk_reads_completed{instance=~"($osd_hosts).*"}[5m]) or irate(node_disk_reads_completed_total{instance=~"($osd_hosts).*"}[5m]) )  + \n(irate(node_disk_writes_completed{instance=~"($osd_hosts).*"}[5m]) or irate(node_disk_writes_completed_total{instance=~"($osd_hosts).*"}[5m])))',
          'time_series',
          12,
          0,
          4,
          5
        ),
        HostsOverviewSingleStatPanel(
          'percent',
          'AVG Disk Utilization',
          'Average Disk utilization for all OSD data devices (i.e. excludes journal/WAL)',
          'current',
          'avg (\n  label_replace((irate(node_disk_io_time_ms[5m]) / 10 ) or\n   (irate(node_disk_io_time_seconds_total[5m]) * 100), "instance", "$1", "instance", "([^.:]*).*"\n  ) *\n  on(instance, device) group_left(ceph_daemon) label_replace(label_replace(ceph_disk_occupation_human{instance=~"($osd_hosts).*"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^.:]*).*")\n)',
          'time_series',
          16,
          0,
          4,
          5
        ),
        HostsOverviewSingleStatPanel(
          'bytes',
          'Network Load',
          'Total send/receive network load across all hosts in the ceph cluster',
          'current',
          |||
            sum (
                    (
                            irate(node_network_receive_bytes{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[1m]) or
                            irate(node_network_receive_bytes_total{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[1m])
                    ) unless on (device, instance)
                    label_replace((bonding_slaves > 0), "device", "$1", "master", "(.+)")
            ) +
            sum (
                    (
                            irate(node_network_transmit_bytes{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[1m]) or
                            irate(node_network_transmit_bytes_total{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[1m])
                    ) unless on (device, instance)
                    label_replace((bonding_slaves > 0), "device", "$1", "master", "(.+)")
                    )
          |||
          ,
          'time_series',
          20,
          0,
          4,
          5
        ),
        HostsOverviewGraphPanel(
          'CPU Busy - Top 10 Hosts',
          'Show the top 10 busiest hosts by cpu',
          'percent',
          'topk(10,100 * ( 1 - (\n    avg by(instance) \n      (irate(node_cpu_seconds_total{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]) or\n       irate(node_cpu{mode=\'idle\',instance=~\"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*\"}[1m]))\n    )\n  )\n)',
          '{{instance}}',
          0,
          5,
          12,
          9
        ),
        HostsOverviewGraphPanel(
          'Network Load - Top 10 Hosts', 'Top 10 hosts by network load', 'Bps', |||
            topk(10, (sum by(instance) (
            (
                    irate(node_network_receive_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[1m]) or
                    irate(node_network_receive_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[1m])
            ) +
            (
                    irate(node_network_transmit_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[1m]) or
                    irate(node_network_transmit_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[1m])
            ) unless on (device, instance)
                    label_replace((bonding_slaves > 0), "device", "$1", "master", "(.+)"))
            ))
          |||
          , '{{instance}}', 12, 5, 12, 9
        ),
      ]),
    'host-details.json':
      local HostDetailsSingleStatPanel(format,
                                       title,
                                       description,
                                       valueName,
                                       expr,
                                       targetFormat,
                                       x,
                                       y,
                                       w,
                                       h) =
        u.addSingleStatSchema(['#299c46', 'rgba(237, 129, 40, 0.89)', '#d44a3a'],
                              '$datasource',
                              format,
                              title,
                              description,
                              valueName,
                              false,
                              100,
                              false,
                              false,
                              '')
        .addTarget(u.addTargetSchema(expr,
                                     1,
                                     targetFormat,
                                     '')) + { gridPos: { x: x, y: y, w: w, h: h } };

      local HostDetailsGraphPanel(alias,
                                  title,
                                  description,
                                  nullPointMode,
                                  formatY1,
                                  labelY1,
                                  expr,
                                  legendFormat,
                                  x,
                                  y,
                                  w,
                                  h) =
        u.graphPanelSchema(alias,
                           title,
                           description,
                           nullPointMode,
                           false,
                           formatY1,
                           'short',
                           labelY1,
                           null,
                           null,
                           1,
                           '$datasource')
        .addTargets(
          [u.addTargetSchema(expr,
                             1,
                             'time_series',
                             legendFormat)]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'Host Details',
        '',
        'rtOg0AiWz',
        'now-1h',
        '10s',
        16,
        ['overview'],
        '',
        {
          refresh_intervals: ['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
      )
      .addRequired(
        type='grafana', id='grafana', name='Grafana', version='5.3.2'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addRequired(
        type='panel', id='singlestat', name='Singlestat', version='5.0.0'
      )
      .addAnnotation(
        u.addAnnotationSchema(
          1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard'
        )
      )
      .addTemplate(
        g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
      )
      .addTemplate(
        u.addTemplateSchema('ceph_hosts', '$datasource', 'label_values(node_scrape_collector_success, instance) ', 1, false, 3, 'Hostname', '([^.:]*).*')
      )
      .addPanels([
        u.addRowSchema(false, true, '$ceph_hosts System Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
        HostDetailsSingleStatPanel(
          'none',
          'OSDs',
          '',
          'current',
          "count(sum by (ceph_daemon) (ceph_osd_metadata{hostname='$ceph_hosts'}))",
          'time_series',
          0,
          1,
          3,
          5
        ),
        HostDetailsGraphPanel(
          {
            interrupt: '#447EBC',
            steal: '#6D1F62',
            system: '#890F02',
            user: '#3F6833',
            wait: '#C15C17',
          }, 'CPU Utilization', "Shows the CPU breakdown. When multiple servers are selected, only the first host's cpu data is shown", 'null', 'percent', '% Utilization', 'sum by (mode) (\n  irate(node_cpu{instance=~"($ceph_hosts)([\\\\.:].*)?", mode=~"(irq|nice|softirq|steal|system|user|iowait)"}[1m]) or\n  irate(node_cpu_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?", mode=~"(irq|nice|softirq|steal|system|user|iowait)"}[1m])\n) / scalar(\n  sum(irate(node_cpu{instance=~"($ceph_hosts)([\\\\.:].*)?"}[1m]) or\n      irate(node_cpu_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[1m]))\n) * 100', '{{mode}}', 3, 1, 6, 10
        ),
        HostDetailsGraphPanel(
          {
            Available: '#508642',
            Free: '#508642',
            Total: '#bf1b00',
            Used: '#bf1b00',
            total: '#bf1b00',
            used: '#0a50a1',
          },
          'RAM Usage',
          '',
          'null',
          'bytes',
          'RAM used',
          'node_memory_MemFree{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_MemFree_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"} ',
          'Free',
          9,
          1,
          6,
          10
        )
        .addTargets(
          [
            u.addTargetSchema('node_memory_MemTotal{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_MemTotal_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"} ', 1, 'time_series', 'total'),
            u.addTargetSchema('(node_memory_Cached{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_Cached_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}) + \n(node_memory_Buffers{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_Buffers_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}) +\n(node_memory_Slab{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_Slab_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}) \n', 1, 'time_series', 'buffers/cache'),
            u.addTargetSchema('(node_memory_MemTotal{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_MemTotal_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"})- (\n  (node_memory_MemFree{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_MemFree_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"})  + \n  (node_memory_Cached{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_Cached_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}) + \n  (node_memory_Buffers{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_Buffers_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}) +\n  (node_memory_Slab{instance=~"$ceph_hosts([\\\\.:].*)?"} or node_memory_Slab_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"})\n  )\n  \n', 1, 'time_series', 'used'),
          ]
        )
        .addSeriesOverride(
          {
            alias: 'total',
            color: '#bf1b00',
            fill: 0,
            linewidth: 2,
            stack: false,
          }
        ),
        HostDetailsGraphPanel(
          {},
          'Network Load',
          "Show the network load (rx,tx) across all interfaces (excluding loopback 'lo')",
          'null',
          'decbytes',
          'Send (-) / Receive (+)',
          'sum by (device) (\n  irate(node_network_receive_bytes{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[1m]) or \n  irate(node_network_receive_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[1m])\n)',
          '{{device}}.rx',
          15,
          1,
          6,
          10
        )
        .addTargets(
          [
            u.addTargetSchema('sum by (device) (\n  irate(node_network_transmit_bytes{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[1m]) or\n  irate(node_network_transmit_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[1m])\n)', 1, 'time_series', '{{device}}.tx'),
          ]
        )
        .addSeriesOverride(
          { alias: '/.*tx/', transform: 'negative-Y' }
        ),
        HostDetailsGraphPanel(
          {},
          'Network drop rate',
          '',
          'null',
          'pps',
          'Send (-) / Receive (+)',
          'irate(node_network_receive_drop{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m]) or irate(node_network_receive_drop_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m])',
          '{{device}}.rx',
          21,
          1,
          3,
          5
        )
        .addTargets(
          [
            u.addTargetSchema(
              'irate(node_network_transmit_drop{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m]) or irate(node_network_transmit_drop_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m])', 1, 'time_series', '{{device}}.tx'
            ),
          ]
        )
        .addSeriesOverride(
          {
            alias: '/.*tx/',
            transform: 'negative-Y',
          }
        ),
        HostDetailsSingleStatPanel(
          'bytes',
          'Raw Capacity',
          'Each OSD consists of a Journal/WAL partition and a data partition. The RAW Capacity shown is the sum of the data partitions across all OSDs on the selected OSD hosts.',
          'current',
          'sum(ceph_osd_stat_bytes and on (ceph_daemon) ceph_disk_occupation{instance=~"($ceph_hosts)([\\\\.:].*)?"})',
          'time_series',
          0,
          6,
          3,
          5
        ),
        HostDetailsGraphPanel(
          {},
          'Network error rate',
          '',
          'null',
          'pps',
          'Send (-) / Receive (+)',
          'irate(node_network_receive_errs{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m]) or irate(node_network_receive_errs_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m])',
          '{{device}}.rx',
          21,
          6,
          3,
          5
        )
        .addTargets(
          [u.addTargetSchema(
            'irate(node_network_transmit_errs{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m]) or irate(node_network_transmit_errs_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[1m])', 1, 'time_series', '{{device}}.tx'
          )]
        )
        .addSeriesOverride(
          {
            alias: '/.*tx/',
            transform: 'negative-Y',
          }
        ),
        u.addRowSchema(false,
                       true,
                       'OSD Disk Performance Statistics') + { gridPos: { x: 0, y: 11, w: 24, h: 1 } },
        HostDetailsGraphPanel(
          {},
          '$ceph_hosts Disk IOPS',
          "For any OSD devices on the host, this chart shows the iops per physical device. Each device is shown by it's name and corresponding OSD id value",
          'connected',
          'ops',
          'Read (-) / Write (+)',
          'label_replace(\n  (\n    irate(node_disk_writes_completed{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) or\n    irate(node_disk_writes_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m])\n  ),\n  "instance",\n  "$1",\n  "instance",\n  "([^:.]*).*"\n)\n* on(instance, device) group_left(ceph_daemon)\n  label_replace(\n    label_replace(\n      ceph_disk_occupation_human,\n      "device",\n      "$1",\n      "device",\n      "/dev/(.*)"\n    ),\n    "instance",\n    "$1",\n    "instance",\n    "([^:.]*).*"\n  )',
          '{{device}}({{ceph_daemon}}) writes',
          0,
          12,
          11,
          9
        )
        .addTargets(
          [
            u.addTargetSchema(
              'label_replace(\n    (irate(node_disk_reads_completed{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) or irate(node_disk_reads_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m])),\n    "instance",\n    "$1",\n    "instance",\n    "([^:.]*).*"\n)\n* on(instance, device) group_left(ceph_daemon)\n  label_replace(\n    label_replace(\n      ceph_disk_occupation_human,\n      "device",\n      "$1",\n      "device",\n      "/dev/(.*)"\n    ),\n    "instance",\n    "$1",\n    "instance",\n    "([^:.]*).*"\n  )',
              1,
              'time_series',
              '{{device}}({{ceph_daemon}}) reads'
            ),
          ]
        )
        .addSeriesOverride(
          { alias: '/.*reads/', transform: 'negative-Y' }
        ),
        HostDetailsGraphPanel(
          {},
          '$ceph_hosts Throughput by Disk',
          'For OSD hosts, this chart shows the disk bandwidth (read bytes/sec + write bytes/sec) of the physical OSD device. Each device is shown by device name, and corresponding OSD id',
          'connected',
          'Bps',
          'Read (-) / Write (+)',
          'label_replace((irate(node_disk_bytes_written{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) or irate(node_disk_written_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m])), "instance", "$1", "instance", "([^:.]*).*") * on(instance, device) group_left(ceph_daemon) label_replace(label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")',
          '{{device}}({{ceph_daemon}}) write',
          12,
          12,
          11,
          9
        )
        .addTargets(
          [u.addTargetSchema(
            'label_replace((irate(node_disk_bytes_read{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) or irate(node_disk_read_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m])), "instance", "$1", "instance", "([^:.]*).*") * on(instance, device) group_left(ceph_daemon) label_replace(label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")',
            1,
            'time_series',
            '{{device}}({{ceph_daemon}}) read'
          )]
        )
        .addSeriesOverride(
          { alias: '/.*read/', transform: 'negative-Y' }
        ),
        HostDetailsGraphPanel(
          {},
          '$ceph_hosts Disk Latency',
          "For OSD hosts, this chart shows the latency at the physical drive. Each drive is shown by device name, with it's corresponding OSD id",
          'null as zero',
          's',
          '',
          'max by(instance,device) (label_replace((irate(node_disk_write_time_seconds_total{ instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) )  / clamp_min(irate(node_disk_writes_completed_total{ instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]), 0.001) or   (irate(node_disk_read_time_seconds_total{ instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) )  / clamp_min(irate(node_disk_reads_completed_total{ instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]), 0.001), "instance", "$1", "instance", "([^:.]*).*")) *  on(instance, device) group_left(ceph_daemon) label_replace(label_replace(ceph_disk_occupation_human{instance=~"($ceph_hosts)([\\\\.:].*)?"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")',
          '{{device}}({{ceph_daemon}})',
          0,
          21,
          11,
          9
        ),
        HostDetailsGraphPanel(
          {},
          '$ceph_hosts Disk utilization',
          'Show disk utilization % (util) of any OSD devices on the host by the physical device name and associated OSD id.',
          'connected',
          'percent',
          '%Util',
          'label_replace(((irate(node_disk_io_time_ms{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) / 10 ) or  irate(node_disk_io_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[5m]) * 100), "instance", "$1", "instance", "([^:.]*).*") * on(instance, device) group_left(ceph_daemon) label_replace(label_replace(ceph_disk_occupation_human{instance=~"($ceph_hosts)([\\\\.:].*)?"}, "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*")',
          '{{device}}({{ceph_daemon}})',
          12,
          21,
          11,
          9
        ),
      ]),
  },
}
