local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'smb-overview.json':
    $.dashboardSchema(
      'SMB Overview',
      '',
      'feem6ehrmi2o0b',
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
      $.addTemplateSchema('Cluster',
                          '$datasource',
                          'label_values(ceph_health_status, cluster)',
                          2,
                          true,
                          0,
                          null,
                          '',
                          current='All')
    )
    .addTemplate(
      $.addTemplateSchema(
        'hostname',
        '$datasource',
        'label_values(smb_metrics_status, instance)',
        1,
        false,
        1,
        null,
        null
      )
    )
    .addPanels([
      $.addStatPanel(
        title='SMB metrics status',
        description="SMB metrics health state. \n1 = OK  \n0 = Failed",
        datasource='${datasource}',
        unit='short',
        gridPosition={ x: 0, y: 0, w: 4, h: 10 },
        graphMode='none',
        colorMode='background',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(smb_metrics_status{instance=~"$hostname"})',
          datasource='${datasource}',
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.addStatPanel(
        title='SMB active sessions',
        description='Number of active SMB sessions, number of users logged in',
        datasource='${datasource}',
        unit='short',
        gridPosition={ x: 4, y: 0, w: 4, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addTargets([
        $.addTargetSchema(
          expr='sum(smb_sessions_total{instance=~"$hostname"})',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.addStatPanel(
        title='SMB ingress throughtput',
        description='Current total throughput, bytes going in per second',
        datasource='${datasource}',
        unit='Bps',
        gridPosition={ x: 8, y: 0, w: 8, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_inbytes{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.addStatPanel(
        title='SMB ingress throughtput - duplicate',
        datasource='${datasource}',
        gridPosition={ x: 16, y: 0, w: 8, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_inbytes{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.addStatPanel(
        title='SMB shares activity',
        description='Number of remote machines using a share',
        datasource='${datasource}',
        gridPosition={ x: 4, y: 5, w: 4, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addTargets([
        $.addTargetSchema(
          expr='sum(smb_share_activity{instance=~"$hostname"})',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.addStatPanel(
        title='SMB latency',
        description='Current total request time for the sum of all runing operations',
        datasource='${datasource}',
        unit="\u00B5s",
        gridPosition={ x: 8, y: 5, w: 8, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_duration_microseconds_sum{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.addStatPanel(
        title='SMB egress throughtput',
        description='Current total egress throughput, bytes going out per second',
        datasource='${datasource}',
        unit='Bps',
        gridPosition={ x: 16, y: 5, w: 8, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='10.4.8',
      ).addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_outbytes{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        )
      ]),
      $.timeSeriesPanel(
        title='SMB throughput',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 10, w: 12, h: 9 },
        lineWidth=1,
        fillOpacity=8,
        pointSize=5,
        showPoints='auto',
        unit='decbytes',
        displayMode='table',
        placement='right',
        tooltip={ mode: 'single', sort: 'none' },
        stackingMode='none',
        decimals=2,
        sortBy='Name',
        sortDesc=false
      )
      .addCalcs(['lastNotNull'])
      .addTargets([
        $.addTargetSchema(
          expr='rate(smb_smb2_request_inbytes{instance=~"$hostname"}[$__rate_interval])',
          datasource='${datasource}',
          instant=false,
          legendFormat='Inbytes.{{instance}}.{{operation}}',
          range=true,
        ),
        $.addTargetSchema(
          expr='rate(smb_smb2_request_outbytes{instance=~"$hostname"}[$__rate_interval])',
          datasource='${datasource}',
          instant=false,
          legendFormat='Outbytes.{{instance}}.{{operation}}',
          range=true,
          hide=false
        ),
      ]),
      $.timeSeriesPanel(
        title='SMB I/O',
        datasource='${datasource}',
        gridPosition={ x: 12, y: 10, w: 12, h: 9 },
        lineWidth=1,
        fillOpacity=8,
        pointSize=5,
        showPoints='auto',
        displayMode='table',
        placement='right',
        tooltip={ mode: 'single', sort: 'none' },
        stackingMode='none',
        decimals=2,
        sortBy='Name',
        sortDesc=false
      )
      .addCalcs(['lastNotNull'])
      .addTargets([
        $.addTargetSchema(
          expr='rate(smb_smb2_request_total{instance=~"$hostname"}[$__rate_interval])',
          datasource='${datasource}',
          instant=false,
          legendFormat='{{instance}}.{{operation}}',
          range=true,
        )
      ]),
      $.timeSeriesPanel(
        title='SMB latencies',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 19, w: 12, h: 9 },
        lineWidth=1,
        fillOpacity=8,
        pointSize=5,
        showPoints='auto',
        unit="s",
        displayMode='table',
        placement='right',
        tooltip={ mode: 'single', sort: 'none' },
        stackingMode='none',
        decimals=2,
        sortBy='Name',
        sortDesc=false
      )
      .addCalcs(['lastNotNull'])
      .addTargets([
        $.addTargetSchema(
          expr='rate(smb_smb2_request_duration_microseconds_sum{instance=~"$hostname"}[$__rate_interval])',
          datasource='${datasource}',
          instant=false,
          legendFormat='{{instance}}.{{operation}}',
          range=true,
        )
      ])
    ])
}
