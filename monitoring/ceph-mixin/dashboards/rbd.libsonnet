local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';
local c = (import '../mixin.libsonnet')._config;

(import 'utils.libsonnet') {
  'rbd-details.json':
    local RbdDetailsPanel(title, formatY1, expr1, expr2, x, y, w, h) =
      $.graphPanelSchema({},
                         title,
                         '',
                         'null as zero',
                         false,
                         formatY1,
                         formatY1,
                         null,
                         null,
                         0,
                         1,
                         '$datasource')
      .addTargets(
        [
          $.addTargetSchema(expr1,
                            '{{pool}} Write'),
          $.addTargetSchema(expr2, '{{pool}} Read'),
        ]
      ) + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RBD Details',
      'Detailed Performance of RBD Images (IOPS/Throughput/Latency)',
      'YhCYGcuZz',
      'now-1h',
      false,
      16,
      $._config.dashboardTags,
      '',
      {
        refresh_intervals: ['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
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
    .addRequired(
      type='grafana', id='grafana', name='Grafana', version='5.3.3'
    )
    .addRequired(
      type='panel', id='graph', name='Graph', version='5.0.0'
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
      $.addTemplateSchema('pool',
                          '$datasource',
                          'label_values(pool)',
                          1,
                          false,
                          0,
                          '',
                          '')
    )
    .addTemplate(
      $.addTemplateSchema('image',
                          '$datasource',
                          'label_values(image)',
                          1,
                          false,
                          0,
                          '',
                          '')
    )
    .addPanels([
      RbdDetailsPanel(
        'IOPS',
        'iops',
        'rate(ceph_rbd_write_ops{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers()
        ,
        'rate(ceph_rbd_read_ops{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers(),
        0,
        0,
        8,
        9
      ),
      RbdDetailsPanel(
        'Throughput',
        'Bps',
        'rate(ceph_rbd_write_bytes{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers(),
        'rate(ceph_rbd_read_bytes{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])' % $.matchers(),
        8,
        0,
        8,
        9
      ),
      RbdDetailsPanel(
        'Average Latency',
        'ns',
        |||
          rate(ceph_rbd_write_latency_sum{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval]) /
            rate(ceph_rbd_write_latency_count{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])
        ||| % $.matchers(),
        |||
          rate(ceph_rbd_read_latency_sum{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval]) /
            rate(ceph_rbd_read_latency_count{%(matchers)s, pool="$pool", image="$image"}[$__rate_interval])
        ||| % $.matchers(),
        16,
        0,
        8,
        9
      ),
    ]),
  'rbd-overview.json':
    local RgwOverviewStyle(alias, pattern, type, unit) =
      $.addStyle(alias,
                 null,
                 ['rgba(245, 54, 54, 0.9)', 'rgba(237, 129, 40, 0.89)', 'rgba(50, 172, 45, 0.97)'],
                 'YYYY-MM-DD HH:mm:ss',
                 2,
                 1,
                 pattern,
                 [],
                 type,
                 unit,
                 []);
    local RbdOverviewPanel(title,
                           formatY1,
                           expr1,
                           expr2,
                           legendFormat1,
                           legendFormat2,
                           x,
                           y,
                           w,
                           h) =
      $.graphPanelSchema({},
                         title,
                         '',
                         'null',
                         false,
                         formatY1,
                         'short',
                         null,
                         null,
                         0,
                         1,
                         '$datasource')
      .addTargets(
        [
          $.addTargetSchema(expr1,
                            legendFormat1),
          $.addTargetSchema(expr2,
                            legendFormat2),
        ]
      ) + { gridPos: { x: x, y: y, w: w, h: h } };

    $.dashboardSchema(
      'RBD Overview',
      '',
      '41FrpeUiz',
      'now-1h',
      '30s',
      16,
      $._config.dashboardTags + ['overview'],
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
    .addRequired(
      type='grafana', id='grafana', name='Grafana', version='5.4.2'
    )
    .addRequired(
      type='panel', id='graph', name='Graph', version='5.0.0'
    )
    .addRequired(
      type='datasource', id='prometheus', name='Prometheus', version='5.0.0'
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
    .addTemplate(
      $.addJobTemplate()
    )
    .addPanels([
      RbdOverviewPanel(
        'IOPS',
        'short',
        'round(sum(rate(ceph_rbd_write_ops{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'round(sum(rate(ceph_rbd_read_ops{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'Writes',
        'Reads',
        0,
        0,
        8,
        7
      ),
      RbdOverviewPanel(
        'Throughput',
        'Bps',
        'round(sum(rate(ceph_rbd_write_bytes{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'round(sum(rate(ceph_rbd_read_bytes{%(matchers)s}[$__rate_interval])))' % $.matchers(),
        'Write',
        'Read',
        8,
        0,
        8,
        7
      ),
      RbdOverviewPanel(
        'Average Latency',
        'ns',
        |||
          round(
            sum(rate(ceph_rbd_write_latency_sum{%(matchers)s}[$__rate_interval])) /
              sum(rate(ceph_rbd_write_latency_count{%(matchers)s}[$__rate_interval]))
          )
        ||| % $.matchers(),
        |||
          round(
            sum(rate(ceph_rbd_read_latency_sum{%(matchers)s}[$__rate_interval])) /
              sum(rate(ceph_rbd_read_latency_count{%(matchers)s}[$__rate_interval]))
          )
        ||| % $.matchers(),
        'Write',
        'Read',
        16,
        0,
        8,
        7
      ),
      $.addTableSchema(
        '$datasource',
        '',
        { col: 3, desc: true },
        [
          RgwOverviewStyle('Pool', 'pool', 'string', 'short'),
          RgwOverviewStyle('Image', 'image', 'string', 'short'),
          RgwOverviewStyle('IOPS', 'Value', 'number', 'iops'),
          RgwOverviewStyle('', '/.*/', 'hidden', 'short'),
        ],
        'Highest IOPS',
        'table'
      )
      .addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              (
                sort((
                  rate(ceph_rbd_write_ops{%(matchers)s}[$__rate_interval]) +
                    on (image, pool, namespace) rate(ceph_rbd_read_ops{%(matchers)s}[$__rate_interval])
                ))
              )
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ) + { gridPos: { x: 0, y: 7, w: 8, h: 7 } },
      $.addTableSchema(
        '$datasource',
        '',
        { col: 3, desc: true },
        [
          RgwOverviewStyle('Pool', 'pool', 'string', 'short'),
          RgwOverviewStyle('Image', 'image', 'string', 'short'),
          RgwOverviewStyle('Throughput', 'Value', 'number', 'Bps'),
          RgwOverviewStyle('', '/.*/', 'hidden', 'short'),
        ],
        'Highest Throughput',
        'table'
      )
      .addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              sort(
                sum(
                  rate(ceph_rbd_read_bytes{%(matchers)s}[$__rate_interval]) +
                    rate(ceph_rbd_write_bytes{%(matchers)s}[$__rate_interval])
                ) by (pool, image, namespace)
              )
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ) + { gridPos: { x: 8, y: 7, w: 8, h: 7 } },
      $.addTableSchema(
        '$datasource',
        '',
        { col: 3, desc: true },
        [
          RgwOverviewStyle('Pool', 'pool', 'string', 'short'),
          RgwOverviewStyle('Image', 'image', 'string', 'short'),
          RgwOverviewStyle('Latency', 'Value', 'number', 'ns'),
          RgwOverviewStyle('', '/.*/', 'hidden', 'short'),
        ],
        'Highest Latency',
        'table'
      )
      .addTarget(
        $.addTargetSchema(
          |||
            topk(10,
              sum(
                rate(ceph_rbd_write_latency_sum{%(matchers)s}[$__rate_interval]) /
                  clamp_min(rate(ceph_rbd_write_latency_count{%(matchers)s}[$__rate_interval]), 1) +
                  rate(ceph_rbd_read_latency_sum{%(matchers)s}[$__rate_interval]) /
                  clamp_min(rate(ceph_rbd_read_latency_count{%(matchers)s}[$__rate_interval]), 1)
              ) by (pool, image, namespace)
            )
          ||| % $.matchers(),
          '',
          'table',
          1,
          true
        )
      ) + { gridPos: { x: 16, y: 7, w: 8, h: 7 } },
    ]),
}
