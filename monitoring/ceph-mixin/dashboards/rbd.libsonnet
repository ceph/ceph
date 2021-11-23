local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';

{
  grafanaDashboards+:: {
    'rbd-details.json':
      local RbdDetailsPanel(title, formatY1, expr1, expr2, x, y, w, h) =
        u.graphPanelSchema({},
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
                           '$Datasource')
        .addTargets(
          [
            u.addTargetSchema(expr1,
                              1,
                              'time_series',
                              '{{pool}} Write'),
            u.addTargetSchema(expr2, 1, 'time_series', '{{pool}} Read'),
          ]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'RBD Details',
        'Detailed Performance of RBD Images (IOPS/Throughput/Latency)',
        'YhCYGcuZz',
        'now-1h',
        false,
        16,
        [],
        '',
        {
          refresh_intervals: ['5s', '10s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
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
      .addRequired(
        type='grafana', id='grafana', name='Grafana', version='5.3.3'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addTemplate(
        g.template.datasource('Datasource', 'prometheus', 'default', label=null)
      )
      .addTemplate(
        u.addTemplateSchema('Pool',
                            '$Datasource',
                            'label_values(pool)',
                            1,
                            false,
                            0,
                            '',
                            '')
      )
      .addTemplate(
        u.addTemplateSchema('Image',
                            '$Datasource',
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
          'irate(ceph_rbd_write_ops{pool="$Pool", image="$Image"}[30s])',
          'irate(ceph_rbd_read_ops{pool="$Pool", image="$Image"}[30s])',
          0,
          0,
          8,
          9
        ),
        RbdDetailsPanel(
          'Throughput',
          'Bps',
          'irate(ceph_rbd_write_bytes{pool="$Pool", image="$Image"}[30s])',
          'irate(ceph_rbd_read_bytes{pool="$Pool", image="$Image"}[30s])',
          8,
          0,
          8,
          9
        ),
        RbdDetailsPanel(
          'Average Latency',
          'ns',
          'irate(ceph_rbd_write_latency_sum{pool="$Pool", image="$Image"}[30s]) / irate(ceph_rbd_write_latency_count{pool="$Pool", image="$Image"}[30s])',
          'irate(ceph_rbd_read_latency_sum{pool="$Pool", image="$Image"}[30s]) / irate(ceph_rbd_read_latency_count{pool="$Pool", image="$Image"}[30s])',
          16,
          0,
          8,
          9
        ),
      ]),
    'rbd-overview.json':
      local RgwOverviewStyle(alias, pattern, type, unit) =
        u.addStyle(alias,
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
        u.graphPanelSchema({},
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
            u.addTargetSchema(expr1,
                              1,
                              'time_series',
                              legendFormat1),
            u.addTargetSchema(expr2,
                              1,
                              'time_series',
                              legendFormat2),
          ]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'RBD Overview',
        '',
        '41FrpeUiz',
        'now-1h',
        '30s',
        16,
        ['overview'],
        '',
        {
          refresh_intervals: ['5s', '10s', '15s', '30s', '1m', '5m', '15m', '30m', '1h', '2h', '1d'],
          time_options: ['5m', '15m', '1h', '6h', '12h', '24h', '2d', '7d', '30d'],
        }
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
        g.template.datasource('datasource',
                              'prometheus',
                              'default',
                              label='Data Source')
      )
      .addPanels([
        RbdOverviewPanel(
          'IOPS',
          'short',
          'round(sum(irate(ceph_rbd_write_ops[30s])))',
          'round(sum(irate(ceph_rbd_read_ops[30s])))',
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
          'round(sum(irate(ceph_rbd_write_bytes[30s])))',
          'round(sum(irate(ceph_rbd_read_bytes[30s])))',
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
          'round(sum(irate(ceph_rbd_write_latency_sum[30s])) / sum(irate(ceph_rbd_write_latency_count[30s])))',
          'round(sum(irate(ceph_rbd_read_latency_sum[30s])) / sum(irate(ceph_rbd_read_latency_count[30s])))',
          'Write',
          'Read',
          16,
          0,
          8,
          7
        ),
        u.addTableSchema(
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
          u.addTargetSchema(
            'topk(10, (sort((irate(ceph_rbd_write_ops[30s]) + on (image, pool, namespace) irate(ceph_rbd_read_ops[30s])))))',
            1,
            'table',
            ''
          )
        ) + { gridPos: { x: 0, y: 7, w: 8, h: 7 } },
        u.addTableSchema(
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
          u.addTargetSchema(
            'topk(10, sort(sum(irate(ceph_rbd_read_bytes[30s]) + irate(ceph_rbd_write_bytes[30s])) by (pool, image, namespace)))',
            1,
            'table',
            ''
          )
        ) + { gridPos: { x: 8, y: 7, w: 8, h: 7 } },
        u.addTableSchema(
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
          u.addTargetSchema(
            'topk(10,\n  sum(\n    irate(ceph_rbd_write_latency_sum[30s]) / clamp_min(irate(ceph_rbd_write_latency_count[30s]), 1) +\n    irate(ceph_rbd_read_latency_sum[30s]) / clamp_min(irate(ceph_rbd_read_latency_count[30s]), 1)\n  ) by (pool, image, namespace)\n)',
            1,
            'table',
            ''
          )
        ) + { gridPos: { x: 16, y: 7, w: 8, h: 7 } },
      ]),
  },
}
