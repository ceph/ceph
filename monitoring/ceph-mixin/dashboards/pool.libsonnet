local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'pool-overview.json':
    local PoolOverviewSingleStatPanel(format,
                                      title,
                                      description,
                                      valueName,
                                      expr,
                                      instant,
                                      targetFormat,
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
      .addTarget($.addTargetSchema(expr, '', targetFormat, 1, instant)) + { gridPos: { x: x, y: y, w: w, h: h } };

    local PoolOverviewStyle(alias,
                            pattern,
                            type,
                            unit,
                            colorMode,
                            thresholds,
                            valueMaps) =
      $.addStyle(alias,
                 colorMode,
                 [
                   'rgba(245, 54, 54, 0.9)',
                   'rgba(237, 129, 40, 0.89)',
                   'rgba(50, 172, 45, 0.97)',
                 ],
                 'YYYY-MM-DD HH:mm:ss',
                 2,
                 1,
                 pattern,
                 thresholds,
                 type,
                 unit,
                 valueMaps);

    local PoolOverviewGraphPanel(title,
                                 description,
                                 formatY1,
                                 labelY1,
                                 expr,
                                 legendFormat,
                                 x,
                                 y,
                                 w,
                                 h) =
      $.graphPanelSchema({},
                         title,
                         description,
                         'null as zero',
                         false,
                         formatY1,
                         'short',
                         labelY1,
                         null,
                         0,
                         1,
                         '$datasource')
      .addTargets(
        [$.addTargetSchema(expr,
                           legendFormat)]
      ) + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'Ceph Pools Overview',
      '',
      'z99hzWtmk',
      'now-1h',
      '15s',
      22,
      $._config.dashboardTags,
      '',
      {
        refresh_intervals: ['5s', '10s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
        time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
      }
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
      $.addJobTemplate()
    )
    .addTemplate(
      g.template.custom(label='TopK',
                        name='topk',
                        current='15',
                        query='15')
    )
    .addPanels([
      PoolOverviewSingleStatPanel(
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
      PoolOverviewSingleStatPanel(
        'none',
        'Pools with Compression',
        'Count of the pools that have compression enabled',
        'current',
        'count(ceph_pool_metadata{%(matchers)s, compression_mode!="none"})' % $.matchers(),
        null,
        '',
        3,
        0,
        3,
        3
      ),
      PoolOverviewSingleStatPanel(
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
      PoolOverviewSingleStatPanel(
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
      PoolOverviewSingleStatPanel(
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
      PoolOverviewSingleStatPanel(
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
      PoolOverviewSingleStatPanel(
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
      PoolOverviewSingleStatPanel(
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
      $.addTableSchema(
        '$datasource',
        '',
        { col: 5, desc: true },
        [
          PoolOverviewStyle('', 'Time', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('', 'instance', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('', 'job', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('Pool Name', 'name', 'string', 'short', null, [], []),
          PoolOverviewStyle('Pool ID', 'pool_id', 'hidden', 'none', null, [], []),
          PoolOverviewStyle('Compression Factor', 'Value #A', 'number', 'none', null, [], []),
          PoolOverviewStyle('% Used', 'Value #D', 'number', 'percentunit', 'value', ['70', '85'], []),
          PoolOverviewStyle('Usable Free', 'Value #B', 'number', 'bytes', null, [], []),
          PoolOverviewStyle('Compression Eligibility', 'Value #C', 'number', 'percent', null, [], []),
          PoolOverviewStyle('Compression Savings', 'Value #E', 'number', 'bytes', null, [], []),
          PoolOverviewStyle('Growth (5d)', 'Value #F', 'number', 'bytes', 'value', ['0', '0'], []),
          PoolOverviewStyle('IOPS', 'Value #G', 'number', 'none', null, [], []),
          PoolOverviewStyle('Bandwidth', 'Value #H', 'number', 'Bps', null, [], []),
          PoolOverviewStyle('', '__name__', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('', 'type', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('', 'compression_mode', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('Type', 'description', 'string', 'short', null, [], []),
          PoolOverviewStyle('Stored', 'Value #J', 'number', 'bytes', null, [], []),
          PoolOverviewStyle('', 'Value #I', 'hidden', 'short', null, [], []),
          PoolOverviewStyle('Compression', 'Value #K', 'string', 'short', null, [], [{ text: 'ON', value: '1' }]),
        ],
        'Pool Overview',
        'table'
      )
      .addTargets(
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
            'ceph_pool_metadata{%(matchers)s, compression_mode!="none"}' % $.matchers(), 'K', 'table', 1, true
          ),
          $.addTargetSchema('', 'L', '', '', null),
        ]
      ) + { gridPos: { x: 0, y: 3, w: 24, h: 6 } },
      PoolOverviewGraphPanel(
        'Top $topk Client IOPS by Pool',
        'This chart shows the sum of read and write IOPS from all clients by pool',
        'short',
        'IOPS',
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
      PoolOverviewGraphPanel(
        'Top $topk Client Bandwidth by Pool',
        'The chart shows the sum of read and write bytes from all clients, by pool',
        'Bps',
        'Throughput',
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
      PoolOverviewGraphPanel(
        'Pool Capacity Usage (RAW)',
        'Historical view of capacity usage, to help identify growth and trends in pool consumption',
        'bytes',
        'Capacity Used',
        'ceph_pool_bytes_used{%(matchers)s} * on(pool_id) group_right ceph_pool_metadata{%(matchers)s}' % $.matchers(),
        '{{name}}',
        0,
        17,
        24,
        7
      ),
    ]),
  'pool-detail.json':
    local PoolDetailSingleStatPanel(format,
                                    title,
                                    description,
                                    valueName,
                                    colorValue,
                                    gaugeMaxValue,
                                    gaugeShow,
                                    sparkLineShow,
                                    thresholds,
                                    expr,
                                    targetFormat,
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
                            colorValue,
                            gaugeMaxValue,
                            gaugeShow,
                            sparkLineShow,
                            thresholds)
      .addTarget($.addTargetSchema(expr, '', targetFormat)) + { gridPos: { x: x, y: y, w: w, h: h } };

    local PoolDetailGraphPanel(alias,
                               title,
                               description,
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
                         'null as zero',
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
      'Ceph Pool Details',
      '',
      '-xyV8KCiz',
      'now-1h',
      '15s',
      22,
      $._config.dashboardTags,
      '',
      {
        refresh_intervals: ['5s', '10s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
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
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
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
      PoolDetailSingleStatPanel(
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
            on(pool_id) group_left(instance, name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
        ||| % $.matchers(),
        'time_series',
        0,
        0,
        7,
        7
      ),
      PoolDetailSingleStatPanel(
        's',
        'Time till full',
        'Time till pool is full assuming the average fill rate of the last 4 hours',
        false,
        100,
        false,
        false,
        '',
        'current',
        |||
          (ceph_pool_max_avail{%(matchers)s} / deriv(ceph_pool_stored{%(matchers)s}[6h])) *
            on(pool_id) group_left(instance, name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"} > 0
        ||| % $.matchers(),
        'time_series',
        7,
        0,
        5,
        7
      ),
      PoolDetailGraphPanel(
        {
          read_op_per_sec:
            '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Object Ingress/Egress',
        '',
        'ops',
        'Objects out(-) / in(+) ',
        |||
          deriv(ceph_pool_objects{%(matchers)s}[1m]) *
            on(pool_id) group_left(instance, name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
        ||| % $.matchers(),
        'Objects per second',
        12,
        0,
        12,
        7
      ),
      PoolDetailGraphPanel(
        {
          read_op_per_sec: '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Client IOPS',
        '',
        'iops',
        'Read (-) / Write (+)',
        |||
          rate(ceph_pool_rd{%(matchers)s}[$__rate_interval]) *
            on(pool_id) group_left(instance,name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
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
              on(pool_id) group_left(instance, name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
          ||| % $.matchers(),
          'writes'
        )
      ),
      PoolDetailGraphPanel(
        {
          read_op_per_sec: '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Client Throughput',
        '',
        'Bps',
        'Read (-) / Write (+)',
        |||
          rate(ceph_pool_rd_bytes{%(matchers)s}[$__rate_interval]) +
            on(pool_id) group_left(instance, name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
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
              on(pool_id) group_left(instance,name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
          ||| % $.matchers(),
          'writes'
        )
      ),
      PoolDetailGraphPanel(
        {
          read_op_per_sec: '#3F6833',
          write_op_per_sec: '#E5AC0E',
        },
        '$pool_name Objects',
        '',
        'short',
        'Objects',
        |||
          ceph_pool_objects{%(matchers)s} *
            on(pool_id) group_left(instance,name) ceph_pool_metadata{%(matchers)s, name=~"$pool_name"}
        ||| % $.matchers(),
        'Number of Objects',
        0,
        14,
        12,
        7
      ),
    ]),
}
