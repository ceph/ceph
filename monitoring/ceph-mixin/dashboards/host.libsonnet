local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'hosts-overview.json':
    local HostsOverviewSingleStatPanel(format,
                                       title,
                                       description,
                                       valueName,
                                       expr,
                                       instant,
                                       x,
                                       y,
                                       w,
                                       h) =
      $.addSingleStatSchema(['#299c46', 'rgba(237, 129, 40, 0.89)', '#d44a3a'],
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
        $.addTargetSchema(expr, '', 'time_series', 1, instant)
      ) + { gridPos: { x: x, y: y, w: w, h: h } };

    local HostsOverviewGraphPanel(title, description, formatY1, expr, legendFormat, x, y, w, h) =
      $.graphPanelSchema(
        {}, title, description, 'null', false, formatY1, 'short', null, null, 0, 1, '$datasource'
      )
      .addTargets(
        [$.addTargetSchema(
          expr, legendFormat
        )]
      ) + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'Host Overview',
      '',
      'y0KGL0iZz',
      'now-1h',
      '10s',
      16,
      $._config.dashboardTags,
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
      $.addAnnotationSchema(
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
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('osd_hosts',
                          '$datasource',
                          'label_values(ceph_disk_occupation{%(matchers)s}, exported_instance)' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          '([^.]*).*')
    )
    .addTemplate(
      $.addTemplateSchema('mon_hosts',
                          '$datasource',
                          'label_values(ceph_mon_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          'mon.(.*)')
    )
    .addTemplate(
      $.addTemplateSchema('mds_hosts',
                          '$datasource',
                          'label_values(ceph_mds_inodes{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          'mds.(.*)')
    )
    .addTemplate(
      $.addTemplateSchema('rgw_hosts',
                          '$datasource',
                          'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
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
        'count(sum by (hostname) (ceph_osd_metadata{%(matchers)s}))' % $.matchers(),
        true,
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
        |||
          avg(1 - (
            avg by(instance) (
              rate(node_cpu_seconds_total{mode='idle',instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}[$__rate_interval]) or
              rate(node_cpu{mode='idle',instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}[$__rate_interval])
            )
          ))
        |||,
        true,
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
        |||
          avg ((
            (
              node_memory_MemTotal{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or
              node_memory_MemTotal_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}
            ) - ((
              node_memory_MemFree{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or
              node_memory_MemFree_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}) +
              (
                node_memory_Cached{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or
                node_memory_Cached_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}
              ) + (
                node_memory_Buffers{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or
                node_memory_Buffers_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}
              ) + (
                node_memory_Slab{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or
                node_memory_Slab_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}
              )
            )
          ) / (
            node_memory_MemTotal{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"} or
             node_memory_MemTotal_bytes{instance=~"($osd_hosts|$rgw_hosts|$mon_hosts|$mds_hosts).*"}
          ))
        |||,
        true,
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
        |||
          sum ((
            rate(node_disk_reads_completed{instance=~"($osd_hosts).*"}[$__rate_interval]) or
            rate(node_disk_reads_completed_total{instance=~"($osd_hosts).*"}[$__rate_interval])
          ) + (
            rate(node_disk_writes_completed{instance=~"($osd_hosts).*"}[$__rate_interval]) or
            rate(node_disk_writes_completed_total{instance=~"($osd_hosts).*"}[$__rate_interval])
          ))
        |||,
        true,
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
        |||
          avg (
            label_replace(
              (rate(node_disk_io_time_ms[$__rate_interval]) / 10 ) or
                (rate(node_disk_io_time_seconds_total[$__rate_interval]) * 100),
              "instance", "$1", "instance", "([^.:]*).*"
            ) * on(instance, device) group_left(ceph_daemon) label_replace(
              label_replace(
                ceph_disk_occupation_human{%(matchers)s, instance=~"($osd_hosts).*"},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^.:]*).*"
            )
          )
        ||| % $.matchers(),
        true,
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
              rate(node_network_receive_bytes{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[$__rate_interval]) or
              rate(node_network_receive_bytes_total{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[$__rate_interval])
            ) unless on (device, instance)
            label_replace((bonding_slaves > 0), "device", "$1", "master", "(.+)")
          ) +
          sum (
            (
              rate(node_network_transmit_bytes{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[$__rate_interval]) or
              rate(node_network_transmit_bytes_total{instance=~"($osd_hosts|mon_hosts|mds_hosts|rgw_hosts).*",device!="lo"}[$__rate_interval])
            ) unless on (device, instance)
            label_replace((bonding_slaves > 0), "device", "$1", "master", "(.+)")
          )
        |||
        ,
        true,
        20,
        0,
        4,
        5
      ),
      HostsOverviewGraphPanel(
        'CPU Busy - Top 10 Hosts',
        'Show the top 10 busiest hosts by cpu',
        'percent',
        |||
          topk(10,
            100 * (
              1 - (
                avg by(instance) (
                  rate(node_cpu_seconds_total{mode='idle',instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}[$__rate_interval]) or
                    rate(node_cpu{mode='idle',instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*"}[$__rate_interval])
                )
              )
            )
          )
        |||,
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
            rate(node_network_receive_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval]) or
            rate(node_network_receive_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval])
          ) +
          (
            rate(node_network_transmit_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval]) or
            rate(node_network_transmit_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval])
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
                                     x,
                                     y,
                                     w,
                                     h) =
      $.addSingleStatSchema(['#299c46', 'rgba(237, 129, 40, 0.89)', '#d44a3a'],
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
      .addTarget($.addTargetSchema(expr)) + { gridPos: { x: x, y: y, w: w, h: h } };

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
      $.graphPanelSchema(alias,
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
        [$.addTargetSchema(expr, legendFormat)]
      ) + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'Host Details',
      '',
      'rtOg0AiWz',
      'now-1h',
      '10s',
      16,
      $._config.dashboardTags + ['overview'],
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
      $.addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard'
      )
    )
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('ceph_hosts',
                          '$datasource',
                          'label_values({%(clusterMatcher)s}, instance)' % $.matchers(),
                          1,
                          false,
                          3,
                          'Hostname',
                          '([^.:]*).*')
    )
    .addPanels([
      $.addRowSchema(false, true, '$ceph_hosts System Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      HostDetailsSingleStatPanel(
        'none',
        'OSDs',
        '',
        'current',
        "count(sum by (ceph_daemon) (ceph_osd_metadata{%(matchers)s, hostname='$ceph_hosts'}))" % $.matchers(),
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
        },
        'CPU Utilization',
        "Shows the CPU breakdown. When multiple servers are selected, only the first host's cpu data is shown",
        'null',
        'percent',
        '% Utilization',
        |||
          sum by (mode) (
            rate(node_cpu{instance=~"($ceph_hosts)([\\\\.:].*)?", mode=~"(irq|nice|softirq|steal|system|user|iowait)"}[$__rate_interval]) or
            rate(node_cpu_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?", mode=~"(irq|nice|softirq|steal|system|user|iowait)"}[$__rate_interval])
          ) / (
            scalar(
              sum(rate(node_cpu{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
              rate(node_cpu_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]))
            ) * 100
          )
        |||,
        '{{mode}}',
        3,
        1,
        6,
        10
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
        |||
          node_memory_MemFree{instance=~"$ceph_hosts([\\\\.:].*)?"} or
            node_memory_MemFree_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
        |||,
        'Free',
        9,
        1,
        6,
        10
      )
      .addTargets(
        [
          $.addTargetSchema(
            |||
              node_memory_MemTotal{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                node_memory_MemTotal_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
            |||,
            'total'
          ),
          $.addTargetSchema(
            |||
              (
                node_memory_Cached{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                node_memory_Cached_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
              ) + (
                node_memory_Buffers{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                node_memory_Buffers_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
              ) + (
                node_memory_Slab{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                node_memory_Slab_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
              )
            |||,
            'buffers/cache'
          ),
          $.addTargetSchema(
            |||
              (
                node_memory_MemTotal{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                node_memory_MemTotal_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
              ) - (
                (
                  node_memory_MemFree{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                  node_memory_MemFree_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
                ) + (
                  node_memory_Cached{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                  node_memory_Cached_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
                ) + (
                  node_memory_Buffers{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                  node_memory_Buffers_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
                ) +
                (
                  node_memory_Slab{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                  node_memory_Slab_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
                )
              )
            |||,
            'used'
          ),
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
        |||
          sum by (device) (
            rate(
              node_network_receive_bytes{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval]) or
              rate(node_network_receive_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval]
            )
          )
        |||,
        '{{device}}.rx',
        15,
        1,
        6,
        10
      )
      .addTargets(
        [
          $.addTargetSchema(
            |||
              sum by (device) (
                rate(node_network_transmit_bytes{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval]) or
                rate(node_network_transmit_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval])
              )
            |||,
            '{{device}}.tx'
          ),
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
        |||
          rate(node_network_receive_drop{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
            rate(node_network_receive_drop_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
        |||,
        '{{device}}.rx',
        21,
        1,
        3,
        5
      )
      .addTargets(
        [
          $.addTargetSchema(
            |||
              rate(node_network_transmit_drop{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
                rate(node_network_transmit_drop_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
            |||,
            '{{device}}.tx'
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
        |||
          sum(
            ceph_osd_stat_bytes{%(matchers)s} and
              on (ceph_daemon) ceph_disk_occupation{%(matchers)s, instance=~"($ceph_hosts)([\\\\.:].*)?"}
          )
        ||| % $.matchers(),
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
        |||
          rate(node_network_receive_errs{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
            rate(node_network_receive_errs_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
        |||,
        '{{device}}.rx',
        21,
        6,
        3,
        5
      )
      .addTargets(
        [$.addTargetSchema(
          |||
            rate(node_network_transmit_errs{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
              rate(node_network_transmit_errs_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
          |||,
          '{{device}}.tx'
        )]
      )
      .addSeriesOverride(
        {
          alias: '/.*tx/',
          transform: 'negative-Y',
        }
      ),
      $.addRowSchema(false,
                     true,
                     'OSD Disk Performance Statistics') + { gridPos: { x: 0, y: 11, w: 24, h: 1 } },
      HostDetailsGraphPanel(
        {},
        '$ceph_hosts Disk IOPS',
        "For any OSD devices on the host, this chart shows the iops per physical device. Each device is shown by it's name and corresponding OSD id value",
        'connected',
        'ops',
        'Read (-) / Write (+)',
        |||
          label_replace(
            (
              rate(node_disk_writes_completed{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
              rate(node_disk_writes_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
            ), "instance", "$1", "instance", "([^:.]*).*"
          ) * on(instance, device) group_left(ceph_daemon) label_replace(
            label_replace(
              ceph_disk_occupation_human{%(matchers)s}, "device", "$1", "device", "/dev/(.*)"
            ), "instance", "$1", "instance", "([^:.]*).*"
          )
        ||| % $.matchers(),
        '{{device}}({{ceph_daemon}}) writes',
        0,
        12,
        11,
        9
      )
      .addTargets(
        [
          $.addTargetSchema(
            |||
              label_replace(
                (
                  rate(node_disk_reads_completed{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
                  rate(node_disk_reads_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
                ), "instance", "$1", "instance", "([^:.]*).*"
              ) * on(instance, device) group_left(ceph_daemon) label_replace(
                label_replace(
                  ceph_disk_occupation_human{%(matchers)s},"device", "$1", "device", "/dev/(.*)"
                ), "instance", "$1", "instance", "([^:.]*).*"
              )
            ||| % $.matchers(),
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
        |||
          label_replace(
            (
              rate(node_disk_bytes_written{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
              rate(node_disk_written_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
            ), "instance", "$1", "instance", "([^:.]*).*") * on(instance, device)
            group_left(ceph_daemon) label_replace(
              label_replace(ceph_disk_occupation_human{%(matchers)s}, "device", "$1", "device", "/dev/(.*)"),
              "instance", "$1", "instance", "([^:.]*).*"
            )
        ||| % $.matchers(),
        '{{device}}({{ceph_daemon}}) write',
        12,
        12,
        11,
        9
      )
      .addTargets(
        [$.addTargetSchema(
          |||
            label_replace(
              (
                rate(node_disk_bytes_read{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
                rate(node_disk_read_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
              ),
              "instance", "$1", "instance", "([^:.]*).*") * on(instance, device)
              group_left(ceph_daemon) label_replace(
                label_replace(ceph_disk_occupation_human{%(matchers)s}, "device", "$1", "device", "/dev/(.*)"),
                "instance", "$1", "instance", "([^:.]*).*"
              )
          ||| % $.matchers(),
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
        |||
          max by(instance, device) (label_replace(
            (rate(node_disk_write_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])) /
              clamp_min(rate(node_disk_writes_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]), 0.001) or
              (rate(node_disk_read_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])) /
                clamp_min(rate(node_disk_reads_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]), 0.001),
            "instance", "$1", "instance", "([^:.]*).*"
          )) * on(instance, device) group_left(ceph_daemon) label_replace(
            label_replace(
              ceph_disk_occupation_human{instance=~"($ceph_hosts)([\\\\.:].*)?"},
              "device", "$1", "device", "/dev/(.*)"
            ), "instance", "$1", "instance", "([^:.]*).*"
          )
        ||| % $.matchers(),
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
        |||
          label_replace(
            (
              (rate(node_disk_io_time_ms{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) / 10) or
              rate(node_disk_io_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) * 100
            ), "instance", "$1", "instance", "([^:.]*).*"
          ) * on(instance, device) group_left(ceph_daemon) label_replace(
            label_replace(ceph_disk_occupation_human{%(matchers)s, instance=~"($ceph_hosts)([\\\\.:].*)?"},
            "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*"
          )
        ||| % $.matchers(),
        '{{device}}({{ceph_daemon}})',
        12,
        21,
        11,
        9
      ),
    ]),
}
