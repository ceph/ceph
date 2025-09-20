local g = import 'grafonnet/grafana.libsonnet';


(import 'utils.libsonnet') {
  'osds-overview.json':
    $.dashboardSchema(
      'OSD Overview',
      '',
      'lo02I1Aiz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags,
      ''
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
    .addRequired(
      type='grafana', id='grafana', name='Grafana', version='5.0.0'
    )
    .addRequired(
      type='panel', id='grafana-piechart-panel', name='Pie Chart', version='1.3.3'
    )
    .addRequired(
      type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addRequired(
      type='panel', id='table', name='Table', version='5.0.0'
    )
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addPanels([
      $.timeSeriesPanel(
        title='OSD Read Latencies',
        datasource='$datasource',
        gridPosition={ x: 0, y: 0, w: 8, h: 8 },
        unit='ms',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            avg(
              rate(ceph_osd_op_r_latency_sum{%(matchers)s}[$__rate_interval]) /
              on (ceph_daemon) rate(ceph_osd_op_r_latency_count{%(matchers)s}[$__rate_interval]) * 1000
            )
          ||| % $.matchers(),
          'AVG read'
        ),
        $.addTargetSchema(
          |||
            max(
              rate(ceph_osd_op_r_latency_sum{%(matchers)s}[$__rate_interval]) /
              on (ceph_daemon) rate(ceph_osd_op_r_latency_count{%(matchers)s}[$__rate_interval]) * 1000
            )
          ||| % $.matchers(),
          'MAX read'
        ),
        $.addTargetSchema(
          |||
            quantile(0.95,
              (
                rate(ceph_osd_op_r_latency_sum{%(matchers)s}[$__rate_interval]) /
                on (ceph_daemon) rate(ceph_osd_op_r_latency_count{%(matchers)s}[$__rate_interval]) * 1000
              )
            )
          ||| % $.matchers(),
          '@95%ile'
        ),
      ]),

      $.addTableExtended(
        datasource='${datasource}',
        title='Highest READ Latencies',
        gridPosition={ h: 8, w: 4, x: 8, y: 0 },
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
            matcher: { id: 'byName', options: 'ceph_daemon' },
            properties: [
              { id: 'displayName', value: 'OSD ID' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Latency (ms)' },
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
        },
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
              (sort(
                (
                  rate(ceph_osd_op_r_latency_sum{%(matchers)s}[$__rate_interval]) /
                    on (ceph_daemon) rate(ceph_osd_op_r_latency_count{%(matchers)s}[$__rate_interval]) *
                    1000
                )
              ))
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),

      $.timeSeriesPanel(
        title='OSD Write Latencies',
        datasource='$datasource',
        gridPosition={ x: 12, y: 0, w: 8, h: 8 },
        unit='ms',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            avg(
              rate(ceph_osd_op_w_latency_sum{%(matchers)s}[$__rate_interval]) /
              on (ceph_daemon) rate(ceph_osd_op_w_latency_count{%(matchers)s}[$__rate_interval]) * 1000
            )
          ||| % $.matchers(),
          'AVG write'
        ),
        $.addTargetSchema(
          |||
            max(
              rate(ceph_osd_op_w_latency_sum{%(matchers)s}[$__rate_interval]) /
              on (ceph_daemon) rate(ceph_osd_op_w_latency_count{%(matchers)s}[$__rate_interval]) * 1000
            )
          ||| % $.matchers(),
          'MAX write'
        ),
        $.addTargetSchema(
          |||
            quantile(0.95,
              (
                rate(ceph_osd_op_w_latency_sum{%(matchers)s}[$__rate_interval]) /
                on (ceph_daemon) rate(ceph_osd_op_w_latency_count{%(matchers)s}[$__rate_interval]) * 1000
              )
            )
          ||| % $.matchers(),
          '@95%ile write'
        ),
      ]),
      $.addTableExtended(
        datasource='${datasource}',
        title='Highest WRITE Latencies',
        description="This table shows the osd's that are delivering the 10 highest write latencies within the cluster",
        gridPosition={ h: 8, w: 4, x: 20, y: 0 },
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
            matcher: { id: 'byName', options: 'ceph_daemon' },
            properties: [
              { id: 'displayName', value: 'OSD ID' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Latency (ms)' },
              { id: 'unit', value: 'none' },
              { id: 'decimals', value: 2 },
              { id: 'custom.align', value: null },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'mappings', value: [{ type: 'value', options: { NaN: { text: '0.00', index: 0 } } }] },
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
        },
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
              (sort(
                (rate(ceph_osd_op_w_latency_sum{%(matchers)s}[$__rate_interval]) /
                  on (ceph_daemon) rate(ceph_osd_op_w_latency_count{%(matchers)s}[$__rate_interval]) *
                  1000)
              ))
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),

      $.pieChartPanel('OSD Types Summary', '', '$datasource', { x: 0, y: 8, w: 4, h: 8 }, 'table', 'bottom', true, ['percent'], { mode: 'single', sort: 'none' }, 'pie', ['percent', 'value'], 'palette-classic')
      .addTarget(
        $.addTargetSchema('count by (device_class) (ceph_osd_metadata{%(matchers)s})' % $.matchers(), '{{device_class}}')
      ),
      $.pieChartPanel('OSD Objectstore Types', '', '$datasource', { x: 4, y: 8, w: 4, h: 8 }, 'table', 'bottom', true, ['percent'], { mode: 'single', sort: 'none' }, 'pie', ['percent', 'value'], 'palette-classic')
      .addTarget($.addTargetSchema(
        'count(ceph_bluefs_wal_total_bytes{%(matchers)s})' % $.matchers(), 'bluestore', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'absent(ceph_bluefs_wal_total_bytes{%(matchers)s}) * count(ceph_osd_metadata{%(matchers)s})' % $.matchers(), 'filestore', 'time_series', 2
      )),
      $.pieChartPanel('OSD Size Summary', 'The pie chart shows the various OSD sizes used within the cluster', '$datasource', { x: 8, y: 8, w: 4, h: 8 }, 'table', 'bottom', true, ['percent'], { mode: 'single', sort: 'none' }, 'pie', ['percent', 'value'], 'palette-classic')
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} < 1099511627776)' % $.matchers(), '<1TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 1099511627776 < 2199023255552)' % $.matchers(), '<2TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 2199023255552 < 3298534883328)' % $.matchers(), '<3TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 3298534883328 < 4398046511104)' % $.matchers(), '<4TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 4398046511104 < 6597069766656)' % $.matchers(), '<6TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 6597069766656 < 8796093022208)' % $.matchers(), '<8TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 8796093022208 < 10995116277760)' % $.matchers(), '<10TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 10995116277760 < 13194139533312)' % $.matchers(), '<12TB', 'time_series', 2
      ))
      .addTarget($.addTargetSchema(
        'count(ceph_osd_stat_bytes{%(matchers)s} >= 13194139533312)' % $.matchers(), '<12TB+', 'time_series', 2
      )),
      g.graphPanel.new(bars=true,
                       datasource='$datasource',
                       title='Distribution of PGs per OSD',
                       x_axis_buckets=20,
                       x_axis_mode='histogram',
                       x_axis_values=['total'],
                       formatY1='short',
                       formatY2='short',
                       labelY1='# of OSDs',
                       min='0',
                       nullPointMode='null')
      .addTarget($.addTargetSchema(
        'ceph_osd_numpg{%(matchers)s}' % $.matchers(), 'PGs per OSD', 'time_series', 1, true
      )) + { type: 'timeseries' } + { fieldConfig: { defaults: { unit: 'short', custom: { fillOpacity: 8, showPoints: 'never' } } } } + { gridPos: { x: 12, y: 8, w: 8, h: 8 } },
      $.gaugeSingleStatPanel(
        'percentunit',
        'OSD onode Hits Ratio',
        'This gauge panel shows onode Hits ratio to help determine if increasing RAM per OSD could help improve the performance of the cluster',
        'current',
        true,
        1,
        true,
        false,
        '.75',
        |||
          sum(ceph_bluestore_onode_hits{%(matchers)s}) / (
            sum(ceph_bluestore_onode_hits{%(matchers)s}) +
            sum(ceph_bluestore_onode_misses{%(matchers)s})
          )
        ||| % $.matchers(),
        'time_series',
        20,
        8,
        4,
        8
      ),
      $.addRowSchema(false,
                     true,
                     'R/W Profile') + { gridPos: { x: 0, y: 16, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='Read/Write Profile',
        datasource='$datasource',
        gridPosition={ x: 0, y: 17, w: 24, h: 8 },
        unit='short',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          'round(sum(rate(ceph_pool_rd{%(matchers)s}[$__rate_interval])))' % $.matchers(),
          'Reads'
        ),
        $.addTargetSchema(
          'round(sum(rate(ceph_pool_wr{%(matchers)s}[$__rate_interval])))' % $.matchers(),
          'Writes'
        ),
      ]),

      $.addTableExtended(
        datasource='${datasource}',
        title='Top Slow Ops',
        description='This table shows the 10 OSDs with the highest number of slow ops',
        gridPosition={ h: 8, w: 5, x: 0, y: 25 },
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
            matcher: { id: 'byName', options: 'ceph_daemon' },
            properties: [
              { id: 'displayName', value: 'OSD ID' },
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
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              Time: true,
              __name__: true,
              instance: true,
              job: true,
              type: true,
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
              (ceph_daemon_health_metrics{type="SLOW_OPS", ceph_daemon=~"osd.*"})
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ),
    ]),
  'osd-device-details.json':
    local OsdDeviceDetailsPanel(title,
                                description,
                                formatY1,
                                labelY1,
                                expr1,
                                expr2,
                                legendFormat1,
                                legendFormat2,
                                x,
                                y,
                                w,
                                h) =
      $.timeSeriesPanel(
        title=title,
        datasource='$datasource',
        gridPosition={ x: x, y: y, w: w, h: h },
        unit=formatY1,
        axisLabel=labelY1,
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(expr1, legendFormat1),
        $.addTargetSchema(expr2, legendFormat2),
      ]);

    $.dashboardSchema(
      'OSD device details',
      '',
      'CrAHE0iZz',
      'now-3h',
      '30s',
      16,
      $._config.dashboardTags,
      ''
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
    .addRequired(
      type='grafana', id='grafana', name='Grafana', version='5.3.2'
    )
    .addRequired(
      type='panel', id='graph', name='Graph', version='5.0.0'
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
      $.addTemplateSchema('osd',
                          '$datasource',
                          'label_values(ceph_osd_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          1,
                          false,
                          1,
                          'OSD',
                          '(.*)')
    )
    .addPanels([
      $.addRowSchema(
        false, true, 'OSD Performance'
      ) + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='$osd Latency',
        datasource='$datasource',
        gridPosition={ x: 0, y: 1, w: 6, h: 9 },
        unit='s',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            rate(ceph_osd_op_r_latency_sum{ceph_daemon=~"$osd", %(matchers)s}[$__rate_interval]) /
              on (ceph_daemon) rate(ceph_osd_op_r_latency_count{%(matchers)s}[$__rate_interval])
          ||| % $.matchers(),
          'read'
        ),
        $.addTargetSchema(
          |||
            rate(ceph_osd_op_w_latency_sum{ceph_daemon=~"$osd", %(matchers)s}[$__rate_interval]) /
              on (ceph_daemon) rate(ceph_osd_op_w_latency_count{%(matchers)s}[$__rate_interval])
          ||| % $.matchers(),
          'write'
        ),
      ])
      .addSeriesOverride({
        alias: 'read',
        transform: 'negative-Y',
      }),
      $.timeSeriesPanel(
        title='$osd R/W IOPS',
        datasource='$datasource',
        gridPosition={ x: 6, y: 1, w: 6, h: 9 },
        unit='short',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          'rate(ceph_osd_op_r{ceph_daemon=~"$osd", %(matchers)s}[$__rate_interval])' % $.matchers(),
          'Reads'
        ),
        $.addTargetSchema(
          'rate(ceph_osd_op_w{ceph_daemon=~"$osd", %(matchers)s}[$__rate_interval])' % $.matchers(),
          'Writes'
        ),
      ])
      .addSeriesOverride({
        alias: 'Reads',
        transform: 'negative-Y',
      }),
      $.timeSeriesPanel(
        title='$osd R/W Bytes',
        datasource='$datasource',
        gridPosition={ x: 12, y: 1, w: 6, h: 9 },
        unit='bytes',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          'rate(ceph_osd_op_r_out_bytes{ceph_daemon=~"$osd", %(matchers)s}[$__rate_interval])' % $.matchers(),
          'Read Bytes'
        ),
        $.addTargetSchema(
          'rate(ceph_osd_op_w_in_bytes{ceph_daemon=~"$osd", %(matchers)s}[$__rate_interval])' % $.matchers(),
          'Write Bytes'
        ),
      ])
      .addSeriesOverride({ alias: 'Read Bytes', transform: 'negative-Y' }),
      $.addRowSchema(
        false, true, 'Physical Device Performance'
      ) + { gridPos: { x: 0, y: 10, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='Physical Device Latency for $osd',
        datasource='$datasource',
        gridPosition={ x: 0, y: 11, w: 6, h: 9 },
        unit='s',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            (
              label_replace(
                rate(node_disk_read_time_seconds_total[$__rate_interval]) /
                  rate(node_disk_reads_completed_total[$__rate_interval]),
                "instance", "$1", "instance", "([^:.]*).*"
              ) and on (instance, device) label_replace(
                label_replace(
                  ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s},
                  "device", "$1", "device", "/dev/(.*)"
                ), "instance", "$1", "instance", "([^:.]*).*"
              )
            )
          ||| % $.matchers(),
          '{{instance}}/{{device}} Reads'
        ),
        $.addTargetSchema(
          |||
            (
              label_replace(
                rate(node_disk_write_time_seconds_total[$__rate_interval]) /
                  rate(node_disk_writes_completed_total[$__rate_interval]),
                "instance", "$1", "instance", "([^:.]*).*") and on (instance, device)
                label_replace(
                  label_replace(
                    ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s}, "device", "$1", "device", "/dev/(.*)"
                  ), "instance", "$1", "instance", "([^:.]*).*"
                )
            )
          ||| % $.matchers(),
          '{{instance}}/{{device}} Writes'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*Reads/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='Physical Device R/W IOPS for $osd',
        datasource='$datasource',
        gridPosition={ x: 6, y: 11, w: 6, h: 9 },
        unit='short',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            label_replace(
              rate(node_disk_writes_completed_total[$__rate_interval]),
              "instance", "$1", "instance", "([^:.]*).*"
            ) and on (instance, device) label_replace(
              label_replace(
                ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}} on {{instance}} Writes'
        ),
        $.addTargetSchema(
          |||
            label_replace(
              rate(node_disk_reads_completed_total[$__rate_interval]),
              "instance", "$1", "instance", "([^:.]*).*"
            ) and on (instance, device) label_replace(
              label_replace(
                ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}} on {{instance}} Reads'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*Reads/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='Physical Device R/W Bytes for $osd',
        datasource='$datasource',
        gridPosition={ x: 12, y: 11, w: 6, h: 9 },
        unit='Bps',
        axisLabel='Read (-) / Write (+)',
        drawStyle='line',
        fillOpacity=8,
        tooltip={ mode: 'multi', sort: 'none' },
        colorMode='palette-classic',
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          |||
            label_replace(
              rate(node_disk_read_bytes_total[$__rate_interval]), "instance", "$1", "instance", "([^:.]*).*"
            ) and on (instance, device) label_replace(
              label_replace(
                ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{instance}} {{device}} Reads'
        ),
        $.addTargetSchema(
          |||
            label_replace(
              rate(node_disk_written_bytes_total[$__rate_interval]), "instance", "$1", "instance", "([^:.]*).*"
            ) and on (instance, device) label_replace(
              label_replace(
                ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s},
                "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{instance}} {{device}} Writes'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*Reads/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='Physical Device Util% for $osd',
        datasource='$datasource',
        gridPosition={ x: 18, y: 11, w: 6, h: 9 },
        unit='percentunit',
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
            label_replace(
              rate(node_disk_io_time_seconds_total[$__rate_interval]),
              "instance", "$1", "instance", "([^:.]*).*"
            ) and on (instance, device) label_replace(
              label_replace(
                ceph_disk_occupation_human{ceph_daemon=~"$osd", %(matchers)s}, "device", "$1", "device", "/dev/(.*)"
              ), "instance", "$1", "instance", "([^:.]*).*"
            )
          ||| % $.matchers(),
          '{{device}} on {{instance}}'
        ),
      ]),
    ]),
}
