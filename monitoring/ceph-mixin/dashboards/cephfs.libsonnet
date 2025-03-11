local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'cephfs-overview.json':
    $.dashboardSchema(
      'MDS Performance',
      '',
      'tbO9LAiZz',
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
      $.addTemplateSchema('mds_servers',
                          '$datasource',
                          'label_values(ceph_mds_inodes{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          1,
                          true,
                          1,
                          'MDS Server',
                          '')
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
    .addPanels([
      $.addRowSchema(false, true, 'MDS Performance') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },
      $.timeSeriesPanel(
        title='MDS Workload - $mds_servers',
        datasource='$datasource',
        gridPosition={ x: 0, y: 1, w: 12, h: 9 },
        axisLabel='Reads(-) / Writes (+)',
        showPoints='never',
        min=0,
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          'sum(rate(ceph_objecter_op_r{ceph_daemon=~"($mds_servers).*", %(matchers)s}[$__rate_interval]))' % $.matchers(),
          'Read Ops'
        ),
        $.addTargetSchema(
          'sum(rate(ceph_objecter_op_w{ceph_daemon=~"($mds_servers).*", %(matchers)s}[$__rate_interval]))' % $.matchers(),
          'Write Ops'
        ),
      ])
      .addSeriesOverride(
        { alias: '/.*Reads/', transform: 'negative-Y' }
      ),
      $.timeSeriesPanel(
        title='Client Request Load - $mds_servers',
        datasource='$datasource',
        gridPosition={ x: 12, y: 1, w: 12, h: 9 },
        axisLabel='Client Requests',
        showPoints='never',
        min=0,
        spanNulls=true,
      )
      .addTargets([
        $.addTargetSchema(
          'ceph_mds_server_handle_client_request{ceph_daemon=~"($mds_servers).*", %(matchers)s}' % $.matchers(),
          '{{ceph_daemon}}'
        ),
      ]),
    ]),
}
