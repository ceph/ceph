local g = import 'grafonnet/grafana.libsonnet';
local u = import 'utils.libsonnet';

{
  grafanaDashboards+:: {
    'cephfs-overview.json':
      local CephfsOverviewGraphPanel(title, formatY1, labelY1, expr, legendFormat, x, y, w, h) =
        u.graphPanelSchema({},
                           title,
                           '',
                           'null',
                           false,
                           formatY1,
                           'short',
                           labelY1,
                           null,
                           0,
                           1,
                           '$datasource')
        .addTargets(
          [u.addTargetSchema(expr, legendFormat)]
        ) + { gridPos: { x: x, y: y, w: w, h: h } };

      u.dashboardSchema(
        'MDS Performance',
        '',
        'tbO9LAiZz',
        'now-1h',
        '15s',
        16,
        [],
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
        type='grafana', id='grafana', name='Grafana', version='5.3.2'
      )
      .addRequired(
        type='panel', id='graph', name='Graph', version='5.0.0'
      )
      .addTemplate(
        g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
      )
      .addTemplate(
        u.addTemplateSchema('mds_servers',
                            '$datasource',
                            'label_values(ceph_mds_inodes, ceph_daemon)',
                            1,
                            true,
                            1,
                            'MDS Server',
                            '')
      )
      .addPanels([
        u.addRowSchema(false, true, 'MDS Performance') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
        CephfsOverviewGraphPanel(
          'MDS Workload - $mds_servers',
          'none',
          'Reads(-) / Writes (+)',
          'sum(rate(ceph_objecter_op_r{ceph_daemon=~"($mds_servers).*"}[1m]))',
          'Read Ops',
          0,
          1,
          12,
          9
        )
        .addTarget(u.addTargetSchema(
          'sum(rate(ceph_objecter_op_w{ceph_daemon=~"($mds_servers).*"}[1m]))',
          'Write Ops'
        ))
        .addSeriesOverride(
          { alias: '/.*Reads/', transform: 'negative-Y' }
        ),
        CephfsOverviewGraphPanel(
          'Client Request Load - $mds_servers',
          'none',
          'Client Requests',
          'ceph_mds_server_handle_client_request{ceph_daemon=~"($mds_servers).*"}',
          '{{ceph_daemon}}',
          12,
          1,
          12,
          9
        ),
      ]),
  },
}
