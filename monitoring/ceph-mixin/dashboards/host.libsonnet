local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'hosts-overview.json':
    $.dashboardSchema(
      'Host Overview',
      '',
      'y0KGL0iZz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags,
      '',
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
    .addLinks([
      $.addLinkSchema(
        asDropdown=true,
        icon='external link',
        includeVars=true,
        keepTime=true,
        tags=[],
        targetBlank=false,
        title='Browse Dashboards',
        tooltip='',
        type='dashboards',
        url=''
      ),
    ])
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
      $.addTemplateSchema('osd_hosts',
                          '$datasource',
                          'label_values(ceph_osd_metadata{%(matchers)s}, hostname)' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          '([^.]*).*')
    )
    .addTemplate(
      $.addTemplateSchema('mon_hosts',
                          '$datasource',
                          'label_values(ceph_mon_metadata{%(matchers)s}, hostname)' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          'mon.(.*)')
    )
    .addTemplate(
      $.addTemplateSchema('mds_hosts',
                          '$datasource',
                          'label_values(ceph_mds_inodes{hostname, %(matchers)s})' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          'mds.(.*)')
    )
    .addTemplate(
      $.addTemplateSchema('rgw_hosts',
                          '$datasource',
                          'label_values(ceph_rgw_metadata{hostname, %(matchers)s})' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          'rgw.(.*)')
    )
    .addPanels([
      $.simpleSingleStatPanel(
        'none',
        'OSD Hosts',
        '',
        'current',
        'count(sum by (hostname) (ceph_osd_metadata{%(matchers)s}))' % $.matchers(),
        true,
        'time_series',
        0,
        0,
        4,
        5
      ),
      $.simpleSingleStatPanel(
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
        'time_series',
        4,
        0,
        4,
        5
      ),
      $.simpleSingleStatPanel(
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
        'time_series',
        8,
        0,
        4,
        5
      ),
      $.simpleSingleStatPanel(
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
        'time_series',
        12,
        0,
        4,
        5
      ),
      $.simpleSingleStatPanel(
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
                ceph_disk_occupation_human{instance=~"($osd_hosts).*", %(matchers)s},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^.:]*).*"
            )
          )
        ||| % $.matchers(),
        true,
        'time_series',
        16,
        0,
        4,
        5
      ),
      $.simpleSingleStatPanel(
        'bytes',
        'Network Load',
        'Total send/receive network load across all hosts in the ceph cluster',
        'current',
        |||
          sum (
            (
              rate(node_network_receive_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval]) or
              rate(node_network_receive_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval])
            ) unless on (device, instance)
            label_replace((node_bonding_slaves > 0), "device", "$1", "master", "(.+)")
          ) +
          sum (
            (
              rate(node_network_transmit_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval]) or
              rate(node_network_transmit_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval])
            ) unless on (device, instance)
            label_replace((node_bonding_slaves > 0), "device", "$1", "master", "(.+)")
          )
        |||,
        true,
        'time_series',
        20,
        0,
        4,
        5
      ),
      $.timeSeriesPanel(
        title='CPU Busy - Top 10 Hosts',
        datasource='$datasource',
        gridPosition={ x: 0, y: 5, w: 12, h: 9 },
        unit='percent',
        axisLabel='',
        min=0,
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
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
          '{{instance}}'
        ),
      ]),
      $.timeSeriesPanel(
        title='Network Load - Top 10 Hosts',
        datasource='$datasource',
        gridPosition={ x: 12, y: 5, w: 12, h: 9 },
        unit='Bps',
        axisLabel='',
        min=0,
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            topk(10, (sum by(instance) (
              (
                rate(node_network_receive_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval]) or
                rate(node_network_receive_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval])
              ) +
              (
                rate(node_network_transmit_bytes{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval]) or
                rate(node_network_transmit_bytes_total{instance=~"($osd_hosts|$mon_hosts|$mds_hosts|$rgw_hosts).*",device!="lo"}[$__rate_interval])
              ) unless on (device, instance)
                label_replace((node_bonding_slaves > 0), "device", "$1", "master", "(.+)"))
            ))
          |||,
          '{{instance}}'
        ),
      ]),
    ]),
  'host-details.json':
    $.dashboardSchema(
      'Host Details',
      '',
      'rtOg0AiWz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags + ['overview'],
      ''
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
    .addLinks([
      $.addLinkSchema(
        asDropdown=true,
        icon='external link',
        includeVars=true,
        keepTime=true,
        tags=[],
        targetBlank=false,
        title='Browse Dashboards',
        tooltip='',
        type='dashboards',
        url=''
      ),
    ])
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('ceph_hosts',
                          '$datasource',
                          'label_values({__name__=~"ceph_.+_metadata", %(matchers)s}, hostname)' % $.matchers(),
                          1,
                          true,
                          1,
                          null,
                          '([^.]*).*')
    )
    .addPanels([
      $.addRowSchema(false, true, '$ceph_hosts System Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      $.simpleSingleStatPanel(
        'none',
        'OSDs',
        '',
        'current',
        'count(sum by (ceph_daemon) (ceph_osd_metadata{%(matchers)s hostname=~"$ceph_hosts"}))' % $.matchers(),
        null,
        'time_series',
        0,
        1,
        3,
        5
      ),
      $.timeSeriesPanel(
        title='CPU Utilization',
        datasource='$datasource',
        gridPosition={ x: 3, y: 1, w: 6, h: 10 },
        unit='percent',
        axisLabel='% Utilization',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
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
          '{{mode}}'
        ),
      ]),
      $.timeSeriesPanel(
        title='RAM Usage',
        datasource='$datasource',
        gridPosition={ x: 9, y: 1, w: 6, h: 10 },
        unit='bytes',
        axisLabel='RAM used',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            node_memory_MemFree{instance=~"$ceph_hosts([\\\\.:].*)?"} or
            node_memory_MemFree_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
          |||,
          'Free'
        ),
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
              ) + (
                node_memory_Slab{instance=~"$ceph_hosts([\\\\.:].*)?"} or
                node_memory_Slab_bytes{instance=~"$ceph_hosts([\\\\.:].*)?"}
              )
            )
          |||,
          'used'
        ),
      ])
      .addSeriesOverride(
        {
          alias: 'total',
          color: '#bf1b00',
          fill: 0,
          linewidth: 2,
          stack: false,
        }
      ),
      $.timeSeriesPanel(
        title='Network Load',
        datasource='$datasource',
        gridPosition={ x: 15, y: 1, w: 6, h: 10 },
        unit='decbytes',
        axisLabel='Send (-) / Receive (+)',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            sum by (device) (
              rate(node_network_receive_bytes{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval]) or
              rate(node_network_receive_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval])
            )
          |||,
          '{{device}}.rx'
        ),
        $.addTargetSchema(
          |||
            sum by (device) (
              rate(node_network_transmit_bytes{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval]) or
              rate(node_network_transmit_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?",device!="lo"}[$__rate_interval])
            )
          |||,
          '{{device}}.tx'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*tx/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='Network drop rate',
        datasource='$datasource',
        gridPosition={ x: 21, y: 1, w: 3, h: 5 },
        unit='pps',
        axisLabel='Send (-) / Receive (+)',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            rate(node_network_receive_drop{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
            rate(node_network_receive_drop_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
          |||,
          '{{device}}.rx'
        ),
        $.addTargetSchema(
          |||
            rate(node_network_transmit_drop{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
            rate(node_network_transmit_drop_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
          |||,
          '{{device}}.tx'
        ),
      ])
      .addSeriesOverride(
        {
          alias: '/.*tx/',
          transform: 'negative-Y',
        }
      ),
      $.simpleSingleStatPanel(
        'bytes',
        'Raw Capacity',
        'Each OSD consists of a Journal/WAL partition and a data partition. The RAW Capacity shown is the sum of the data partitions across all OSDs on the selected OSD hosts.',
        'current',
        |||
          sum(
            ceph_osd_stat_bytes{%(matchers)s} and
              on (ceph_daemon) ceph_disk_occupation{instance=~"($ceph_hosts)([\\\\.:].*)?", %(matchers)s}
          )
        ||| % $.matchers(),
        null,
        'time_series',
        0,
        6,
        3,
        5
      ),
      $.timeSeriesPanel(
        title='Network error rate',
        datasource='$datasource',
        gridPosition={ x: 21, y: 6, w: 3, h: 5 },
        unit='pps',
        axisLabel='Send (-) / Receive (+)',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            rate(node_network_receive_errs{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
            rate(node_network_receive_errs_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
          |||,
          '{{device}}.rx'
        ),
        $.addTargetSchema(
          |||
            rate(node_network_transmit_errs{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval]) or
            rate(node_network_transmit_errs_total{instance=~"$ceph_hosts([\\\\.:].*)?"}[$__rate_interval])
          |||,
          '{{device}}.tx'
        ),
      ])
      .addSeriesOverride(
        {
          alias: '/.*tx/',
          transform: 'negative-Y',
        }
      ),
      $.addRowSchema(
        false,
        true,
        'OSD Disk Performance Statistics'
      ) + { gridPos: { x: 0, y: 11, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='$ceph_hosts Disk IOPS',
        datasource='$datasource',
        gridPosition={ x: 0, y: 12, w: 11, h: 9 },
        unit='ops',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
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
          '{{device}}({{ceph_daemon}}) writes'
        ),
        $.addTargetSchema(
          |||
            label_replace(
              (
                rate(node_disk_reads_completed{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
                rate(node_disk_reads_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
              ), "instance", "$1", "instance", "([^:.]*).*"
            ) * on(instance, device) group_left(ceph_daemon) label_replace(
              label_replace(
                ceph_disk_occupation_human{%(matchers)s}, "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}}({{ceph_daemon}}) reads'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*reads/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='$ceph_hosts Throughput by Disk',
        datasource='$datasource',
        gridPosition={ x: 12, y: 12, w: 11, h: 9 },
        unit='Bps',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            label_replace(
              (
                rate(node_disk_bytes_written{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
                rate(node_disk_written_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
              ), "instance", "$1", "instance", "([^:.]*).*"
            ) * on(instance, device)
            group_left(ceph_daemon) label_replace(
              label_replace(ceph_disk_occupation_human{%(matchers)s}, "device", "$1", "device", "/dev/(.*)"),
              "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}}({{ceph_daemon}}) write'
        ),
        $.addTargetSchema(
          |||
            label_replace(
              (
                rate(node_disk_bytes_read{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) or
                rate(node_disk_read_bytes_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])
              ), "instance", "$1", "instance", "([^:.]*).*"
            ) * on(instance, device)
            group_left(ceph_daemon) label_replace(
              label_replace(ceph_disk_occupation_human{%(matchers)s}, "device", "$1", "device", "/dev/(.*)"),
              "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}}({{ceph_daemon}}) read'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*read/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='$ceph_hosts Disk Latency',
        datasource='$datasource',
        gridPosition={ x: 0, y: 21, w: 11, h: 9 },
        unit='s',
        axisLabel='',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            max by(instance, device) (
              label_replace(
                (
                  (rate(node_disk_write_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])) /
                  clamp_min(rate(node_disk_writes_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]), 0.001) or
                  (rate(node_disk_read_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval])) /
                  clamp_min(rate(node_disk_reads_completed_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]), 0.001)
                ),
                "instance", "$1", "instance", "([^:.]*).*"
              )
            ) * on(instance, device) group_left(ceph_daemon) label_replace(
              label_replace(
                ceph_disk_occupation_human{instance=~"($ceph_hosts)([\\\\.:].*)?"},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}}({{ceph_daemon}})'
        ),
      ]),
      $.timeSeriesPanel(
        title='$ceph_hosts Disk utilization',
        datasource='$datasource',
        gridPosition={ x: 12, y: 21, w: 11, h: 9 },
        unit='percent',
        axisLabel='%Util',
        drawStyle='line',
        fillOpacity=8,
        showPoints='never',
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            label_replace(
              (
                (rate(node_disk_io_time_ms{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) / 10) or
                rate(node_disk_io_time_seconds_total{instance=~"($ceph_hosts)([\\\\.:].*)?"}[$__rate_interval]) * 100
              ), "instance", "$1", "instance", "([^:.]*).*"
            ) * on(instance, device) group_left(ceph_daemon) label_replace(
              label_replace(ceph_disk_occupation_human{instance=~"($ceph_hosts)([\\\\.:].*)?", %(matchers)s},
              "device", "$1", "device", "/dev/(.*)"), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}}({{ceph_daemon}})'
        ),
      ]),

      $.addTableExtended(
        datasource='${datasource}',
        title='Top Slow Ops per Host',
        gridPosition={ h: 8, w: 6, x: 0, y: 30 },
        options={
          footer: {
            fields: '',
            reducer: ['sum'],
            countRows: false,
            enablePagination: false,
            show: false,
          },
          frameIndex: 1,
          showHeader: true,
        },
        custom={ align: 'null', cellOptions: { type: 'auto' }, filterable: true, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
            { color: 'red', value: 80 },
          ],
        },
        overrides=[
          {
            matcher: { id: 'byName', options: 'instance' },
            properties: [
              { id: 'displayName', value: 'Instance' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Slow Ops' },
              { id: 'unit', value: 'none' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
        ],
        pluginVersion='10.4.0'
      )
      .addTransformations([
        {
          id: 'merge',
          options: { reducers: [] },
        }
        {
          id: 'organize',
          options: {
            excludeByName: {
              Time: true,
              cluster: true,
            },
            indexByName: {},
            renameByName: {},
            includeByName: {},
          },
        },
      ]).addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              (sum by (instance)(ceph_daemon_health_metrics{type="SLOW_OPS", ceph_daemon=~"osd.*", %(matchers)s}))
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),
    ]),
}
