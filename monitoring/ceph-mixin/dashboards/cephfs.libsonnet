local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'cephfs-overview.json':
    $.dashboardSchema(
      'MDS Performance',
      '',
      'tbO9LAiZz',
      'now-1h',
      '15s',
      16,
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
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addJobTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('mds_servers',
                          '$datasource',
                          'label_values(ceph_mds_inodes{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          1,
                          true,
                          1,
                          'MDS Server',
                          '')
    )
    .addPanels([
      $.addRowSchema(false, true, 'MDS Performance') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      $.simpleGraphPanel(
        {},
        'MDS Workload - $mds_servers',
        '',
        'none',
        'Reads(-) / Writes (+)',
        0,
        'sum(rate(ceph_objecter_op_r{%(matchers)s, ceph_daemon=~"($mds_servers).*"}[$__rate_interval]))' % $.matchers(),
        'Read Ops',
        0,
        1,
        12,
        9
      )
      .addTarget($.addTargetSchema(
        'sum(rate(ceph_objecter_op_w{%(matchers)s, ceph_daemon=~"($mds_servers).*"}[$__rate_interval]))' % $.matchers(),
        'Write Ops'
      ))
      .addSeriesOverride(
        { alias: '/.*Reads/', transform: 'negative-Y' }
      ),
      $.simpleGraphPanel(
        {},
        'Client Request Load - $mds_servers',
        '',
        'none',
        'Client Requests',
        0,
        'ceph_mds_server_handle_client_request{%(matchers)s, ceph_daemon=~"($mds_servers).*"}' % $.matchers(),
        '{{ceph_daemon}}',
        12,
        1,
        12,
        9
      ),
    ]),
}
