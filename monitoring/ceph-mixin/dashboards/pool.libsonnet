local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'pool-overview.json':
    $.dashboardSchema(
      'Ceph Pools Overview',
      '',
      'z99hzWtmk',
      'now-1h',
      '30s',
      22,
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
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      g.template.custom(label='TopK',
                        name='topk',
                        current='15',
                        query='15')
    )
    .addPanels([
      $.simpleSingleStatPanel(
        'none',
        'Pools',
        '',
        'avg',
        'count(ceph_pool_metadata{%(matchers)s})' % $.matchers(),
        true,
        'table',
        0,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'none',
        'Pools with Compression',
        'Count of the pools that have compression enabled',
        'current',
        'count(ceph_pool_metadata{compression_mode!="none", %(matchers)s})' % $.matchers(),
        null,
        '',
        3,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'bytes',
        'Total Raw Capacity',
        'Total raw capacity available to the cluster',
        'current',
        'sum(ceph_osd_stat_bytes{%(matchers)s})' % $.matchers(),
        null,
        '',
        6,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'bytes',
        'Raw Capacity Consumed',
        'Total raw capacity consumed by user data and associated overheads (metadata + redundancy)',
        'current',
        'sum(ceph_pool_bytes_used{%(matchers)s})' % $.matchers(),
        true,
        '',
        9,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'bytes',
        'Logical Stored ',
        'Total of client data stored in the cluster',
        'current',
        'sum(ceph_pool_stored{%(matchers)s})' % $.matchers(),
        true,
        '',
        12,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'bytes',
        'Compression Savings',
        'A compression saving is determined as the data eligible to be compressed minus the capacity used to store the data after compression',
        'current',
        |||
          sum(
            ceph_pool_compress_under_bytes{%(matchers)s} -
              ceph_pool_compress_bytes_used{%(matchers)s}
          )
        ||| % $.matchers(),
        null,
        '',
        15,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'percent',
        'Compression Eligibility',
        'Indicates how suitable the data is within the pools that are/have been enabled for compression - averaged across all pools holding compressed data',
        'current',
        |||
          (
            sum(ceph_pool_compress_under_bytes{%(matchers)s} > 0) /
              sum(ceph_pool_stored_raw{%(matchers)s} and ceph_pool_compress_under_bytes{%(matchers)s} > 0)
          ) * 100
        ||| % $.matchers(),
        null,
        'table',
        18,
        0,
        3,
        3
      ),
      $.simpleSingleStatPanel(
        'none',
        'Compression Factor',
        'This factor describes the average ratio of data eligible to be compressed divided by the data actually stored. It does not account for data written that was ineligible for compression (too small, or compression yield too low)',
        'current',
        |||
          sum(
            ceph_pool_compress_under_bytes{%(matchers)s} > 0)
              / sum(ceph_pool_compress_bytes_used{%(matchers)s} > 0
          )
        ||| % $.matchers(),
        null,
        '',
        21,
        0,
        3,
        3
      ),

      $.addTableExtended(
        datasource='${datasource}',
        title='Pool Overview',
        gridPosition={ h: 6, w: 24, x: 0, y: 3 },
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
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: true, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'green', value: null },
            { color: 'red', value: 80 },
          ],
        },
        overrides=[
          {
            matcher: { id: 'byName', options: 'Time' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'instance' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'job' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'name' },
            properties: [
              { id: 'displayName', value: 'Pool Name' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'pool_id' },
            properties: [
              { id: 'displayName', value: 'Pool ID' },
              { id: 'unit', value: 'none' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #A' },
            properties: [
              { id: 'displayName', value: 'Compression Factor' },
              { id: 'unit', value: 'none' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #D' },
            properties: [
              { id: 'displayName', value: '% Used' },
              { id: 'unit', value: 'percentunit' },
              { id: 'decimals', value: 2 },
              { id: 'custom.cellOptions', value: { type: 'color-text' } },
              {
                id: 'thresholds',
                value: {
                  mode: 'absolute',
                  steps: [
                    {
                      color: 'rgba(245, 54, 54, 0.9)',
                      value: null,
                    },
                    {
                      color: 'rgba(237, 129, 40, 0.89)',
                      value: 70,
                    },
                    {
                      color: 'rgba(50, 172, 45, 0.97)',
                      value: 85,
                    },
                  ],
                },
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #B' },
            properties: [
              { id: 'displayName', value: 'Usable Free' },
              { id: 'unit', value: 'bytes' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #C' },
            properties: [
              { id: 'displayName', value: 'Compression Eligibility' },
              { id: 'unit', value: 'percent' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #E' },
            properties: [
              { id: 'displayName', value: 'Compression Savings' },
              { id: 'unit', value: 'bytes' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #F' },
            properties: [
              { id: 'displayName', value: 'Growth (5d)' },
              { id: 'unit', value: 'bytes' },
              { id: 'decimals', value: 2 },
              { id: 'custom.cellOptions', value: { type: 'color-text' } },
              {
                id: 'thresholds',
                value: {
                  mode: 'absolute',
                  steps: [
                    {
                      color: 'rgba(245, 54, 54, 0.9)',
                      value: null,
                    },
                    {
                      color: 'rgba(237, 129, 40, 0.89)',
                      value: 70,
                    },
                    {
                      color: 'rgba(50, 172, 45, 0.97)',
                      value: 85,
                    },
                  ],
                },
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #G' },
            properties: [
              { id: 'displayName', value: 'IOPS' },
              { id: 'unit', value: 'none' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #H' },
            properties: [
              { id: 'displayName', value: 'Bandwidth' },
              { id: 'unit', value: 'Bps' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: '__name__' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'type' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'compression_mode' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'description' },
            properties: [
              { id: 'displayName', value: 'Type' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #J' },
            properties: [
              { id: 'displayName', value: 'Stored' },
              { id: 'unit', value: 'bytes' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #I' },
            properties: [
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #K' },
            properties: [
              { id: 'displayName', value: 'Compression' },
              { id: 'unit', value: 'short' },
              { id: 'decimals', value: 2 },
            ],
          },
        ],
        pluginVersion='10.4.0'
      )
      .addTransformations([
        {
          id: 'merge',
          options: {},
        },
        {
          id: 'seriesToRows',
          options: {},
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              Time: true,
              'Value #A': true,
              instance: true,
              job: true,
              pool_id: true,
              'Value #B': false,
              'Value #C': true,
              __name__: true,
              compression_mode: true,
              type: true,
              'Value #I': true,
              'Value #K': true,
              'Value #D': false,
              'Value #E': true,
              cluster: true,
            },
            indexByName: {},
            renameByName: {},
            includeByName: {},
          },
        },
      ]).addTargets(
        [
          $.addTargetSchema(
            |||
              (
                ceph_pool_compress_under_bytes{%(matchers)s} /
                  ceph_pool_compress_bytes_used{%(matchers)s} > 0
              ) and on(pool_id) (
                (
                  (ceph_pool_compress_under_bytes{%(matchers)s} > 0) /
                    ceph_pool_stored_raw{%(matchers)s}
                ) * 100 > 0.5
              )
            ||| % $.matchers(),
            'A',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            |||
              ceph_pool_max_avail{%(matchers)s} *
                on(pool_id) group_left(name) ceph_pool_metadata{%(matchers)s}
            ||| % $.matchers(),
            'B',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            |||
              (
                (ceph_pool_compress_under_bytes{%(matchers)s} > 0) /
                  ceph_pool_stored_raw{%(matchers)s}
              ) * 100
            ||| % $.matchers(),
            'C',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            |||
              ceph_pool_percent_used{%(matchers)s} *
                on(pool_id) group_left(name) ceph_pool_metadata{%(matchers)s}
            ||| % $.matchers(),
            'D',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            |||
              ceph_pool_compress_under_bytes{%(matchers)s} -
                ceph_pool_compress_bytes_used{%(matchers)s} > 0
            ||| % $.matchers(),
            'E',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            'delta(ceph_pool_stored{%(matchers)s}[5d])' % $.matchers(), 'F', 'table', 1, true
          ),
          $.addTargetSchema(
            |||
              rate(ceph_pool_rd{%(matchers)s}[$__rate_interval])
                + rate(ceph_pool_wr{%(matchers)s}[$__rate_interval])
            ||| % $.matchers(),
            'G',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            |||
              rate(ceph_pool_rd_bytes{%(matchers)s}[$__rate_interval]) +
                rate(ceph_pool_wr_bytes{%(matchers)s}[$__rate_interval])
            ||| % $.matchers(),
            'H',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            'ceph_pool_metadata{%(matchers)s}' % $.matchers(), 'I', 'table', 1, true
          ),
          $.addTargetSchema(
            'ceph_pool_stored{%(matchers)s} * on(pool_id) group_left ceph_pool_metadata{%(matchers)s}' % $.matchers(),
            'J',
            'table',
            1,
            true
          ),
          $.addTargetSchema(
            'ceph_pool_metadata{compression_mode!="none", %(matchers)s}' % $.matchers(), 'K', 'table', 1, true
          ),
          $.addTargetSchema('', 'L', '', '', null),
        ]
      ),

      $.simpleGraphPanel(
        {},
        'Top $topk Client IOPS by Pool',
        'This chart shows the sum of read and write IOPS from all clients by pool',
        'short',
        'IOPS',
        0,
        |||
          topk($topk,
            round(
              (
                rate(ceph_pool_rd{%(matchers)s}[$__rate_interval]) +
                  rate(ceph_pool_wr{%(matchers)s}[$__rate_interval])
              ), 1
            ) * on(pool_id) group_left(instance,name) ceph_pool_metadata{%(matchers)s})
        ||| % $.matchers(),
        '{{name}} ',
        0,
        9,
        12,
        8
      )
      .addTarget(
        $.addTargetSchema(
          |||
            topk($topk,
              rate(ceph_pool_wr{%(matchers)s}[$__rate_interval]) +
                on(pool_id) group_left(instance,name) ceph_pool_metadata{%(matchers)s}
            )
          ||| % $.matchers(),
          '{{name}} - write'
        )
      ),
      $.simpleGraphPanel(
        {},
        'Top $topk Client Bandwidth by Pool',
        'The chart shows the sum of read and write bytes from all clients, by pool',
        'Bps',
        'Throughput',
        0,
        |||
          topk($topk,
            (
              rate(ceph_pool_rd_bytes{%(matchers)s}[$__rate_interval]) +
                rate(ceph_pool_wr_bytes{%(matchers)s}[$__rate_interval])
            ) * on(pool_id) group_left(instance, name) ceph_pool_metadata{%(matchers)s}
          )
        ||| % $.matchers(),
        '{{name}}',
        12,
        9,
        12,
        8
      ),
      $.simpleGraphPanel(
        {},
        'Pool Capacity Usage (RAW)',
        'Historical view of capacity usage, to help identify growth and trends in pool consumption',
        'bytes',
        'Capacity Used',
        0,
        'ceph_pool_bytes_used{%(matchers)s} * on(pool_id) group_right ceph_pool_metadata{%(matchers)s}' % $.matchers(),
        '{{name}}',
        0,
        17,
        24,
        7
      ),
    ]),
  'pool-detail.json':
    $.dashboardSchema(
      'Ceph Pool Details',
      '',
      '-xyV8KCiz',
      'now-1h',
      '30s',
      22,
      $._config.dashboardTags,
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
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('pool_name',
                          '$datasource',
                          'label_values(ceph_pool_metadata{%(matchers)s}, name)' % $.matchers(),
                          1,
                          false,
                          1,
                          'Pool Name',
                          '')
    )
    .addPanels([
      $.gaugeSingleStatPanel(
        'percentunit',
        'Capacity used',
        '',
        'current',
        true,
        1,
        true,
        true,
        '.7,.8',
        |||
          (ceph_pool_stored{%(matchers)s} / (ceph_pool_stored{%(matchers)s} + ceph_pool_max_avail{%(matchers)s})) *
            on(pool_id) group_left(instance, name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
        ||| % $.matchers(),
        'time_series',
        0,
        0,
        7,
        7
      ),
      $.gaugeSingleStatPanel(
        's',
        'Time till full',
        'Time till pool is full assuming the average fill rate of the last 6 hours',
        false,
        100,
        false,
        false,
        '',
        'current',
        |||
          (ceph_pool_max_avail{%(matchers)s} / deriv(ceph_pool_stored{%(matchers)s}[6h])) *
            on(pool_id) group_left(instance, name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s} > 0
        ||| % $.matchers(),
        'time_series',
        7,
        0,
        5,
        7
      ),
      $.simpleGraphPanel(
        {
          read_op_per_sec:
            '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Object Ingress/Egress',
        '',
        'ops',
        'Objects out(-) / in(+) ',
        null,
        |||
          deriv(ceph_pool_objects{%(matchers)s}[1m]) *
            on(pool_id) group_left(instance, name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
        ||| % $.matchers(),
        'Objects per second',
        12,
        0,
        12,
        7
      ),
      $.simpleGraphPanel(
        {
          read_op_per_sec: '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Client IOPS',
        '',
        'iops',
        'Read (-) / Write (+)',
        null,
        |||
          rate(ceph_pool_rd{%(matchers)s}[$__rate_interval]) *
            on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
        ||| % $.matchers(),
        'reads',
        0,
        7,
        12,
        7
      )
      .addSeriesOverride({ alias: 'reads', transform: 'negative-Y' })
      .addTarget(
        $.addTargetSchema(
          |||
            rate(ceph_pool_wr{%(matchers)s}[$__rate_interval]) *
              on(pool_id) group_left(instance, name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
          ||| % $.matchers(),
          'writes'
        )
      ),
      $.simpleGraphPanel(
        {
          read_op_per_sec: '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Client Throughput',
        '',
        'Bps',
        'Read (-) / Write (+)',
        null,
        |||
          rate(ceph_pool_rd_bytes{%(matchers)s}[$__rate_interval]) +
            on(pool_id) group_left(instance, name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
        ||| % $.matchers(),
        'reads',
        12,
        7,
        12,
        7
      )
      .addSeriesOverride({ alias: 'reads', transform: 'negative-Y' })
      .addTarget(
        $.addTargetSchema(
          |||
            rate(ceph_pool_wr_bytes{%(matchers)s}[$__rate_interval]) +
              on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
          ||| % $.matchers(),
          'writes'
        )
      ),
      $.simpleGraphPanel(
        {
          read_op_per_sec: '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Objects',
        '',
        'short',
        'Objects',
        null,
        |||
          ceph_pool_objects{%(matchers)s} *
            on(pool_id) group_left(instance,name) ceph_pool_metadata{name=~"$pool_name", %(matchers)s}
        ||| % $.matchers(),
        'Number of Objects',
        0,
        14,
        12,
        7
      ),
    ]),
}
