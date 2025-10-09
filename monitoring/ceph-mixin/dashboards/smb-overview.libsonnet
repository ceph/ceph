local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'smb-overview.json':
    $.dashboardSchema(
      'SMB Overview',
      'SMB Overview dashboard shows data across all clusters and hosts associated with the SMB service.',
      'feem6ehrmi2o0b',
      'now-6h',
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
                          false,
                          0,
                          null,
                          '')
    )
    .addTemplate(
      $.addTemplateSchema(
        'SMBcluster',
        '$datasource',
        'label_values(smb_metrics_status,netbiosname)',
        1,
        true,
        1,
        'SMB Cluster',
        null,
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'hostname',
        '$datasource',
        'label_values(smb_metrics_status{netbiosname=~"$SMBcluster"},instance)',
        1,
        true,
        1,
        'Hostname',
        null,
        current='All'
      )
    )
    .addPanels([
      $.addStatPanel(
        title='Prometheus SMB metrics status',
        description='SMB metrics daemon health.',
        datasource='${datasource}',
        unit='short',
        gridPosition={ x: 0, y: 0, w: 8, h: 8 },
        graphMode='none',
        colorMode='background',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      )
      .addMappings([
        {
          options: {
            '0': { text: 'Down', color: 'red' },
            '1': { text: 'Up', color: 'green' },
          },
          type: 'value',
        },
      ]).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(smb_metrics_status{instance=~"$hostname"})  by (instance)',
          datasource='${datasource}',
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Nodes per Cluster',
        description='Number of nodes per cluster',
        datasource='${datasource}',
        unit='none',
        gridPosition={ x: 8, y: 0, w: 8, h: 4 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count by (netbiosname) (smb_sessions_total * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"})',
          datasource='${datasource}',
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Active sessions per Cluster',
        description='Number of users currently logged in Cluster',
        datasource='${datasource}',
        unit='none',
        gridPosition={ x: 16, y: 0, w: 8, h: 4 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum by(netbiosname)(smb_sessions_total * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"}) / (count by ( netbiosname)(smb_sessions_total * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"}))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Shares activity per Cluster',
        description='Number of remote machines using a share in cluster',
        datasource='${datasource}',
        gridPosition={ x: 8, y: 4, w: 8, h: 4 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum by(netbiosname)(smb_share_activity * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"}) / (count by ( netbiosname)(smb_share_activity * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"}))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Active users per Cluster',
        description='Number of currently active SMB user per cluster',
        datasource='${datasource}',
        gridPosition={ x: 16, y: 4, w: 8, h: 4 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum by(netbiosname)(smb_users_total * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"}) / (count by ( netbiosname)(smb_users_total * on (instance) group_left (netbiosname) smb_metrics_status{netbiosname=~"$SMBcluster"}))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Ingress throughtput',
        description='Current total ingress throughput, bytes going in per second',
        datasource='${datasource}',
        unit='decbytes',
        gridPosition={ x: 0, y: 8, w: 6, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_inbytes{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Latency',
        description='Current total request time for the sum of all runing operations',
        datasource='${datasource}',
        unit='µs',
        gridPosition={ x: 6, y: 8, w: 6, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_duration_microseconds_sum{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.addStatPanel(
        title='Egress throughtput',
        description='Current total egress throughput, bytes going out per second',
        datasource='${datasource}',
        unit='decbytes',
        gridPosition={ x: 12, y: 8, w: 6, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_outbytes{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='I/O',
        description='Current total number of operations per second',
        datasource='${datasource}',
        unit='short',
        gridPosition={ x: 18, y: 8, w: 6, h: 5 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).
        addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(rate(smb_smb2_request_outbytes{instance=~"$hostname"}[$__rate_interval]))',
          datasource='${datasource}',
          exemplar=false,
          instant=false,
          legendFormat='__auto',
          range=true
        ),
      ]),
      $.timeSeriesPanel(
        title='Throughput',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 13, w: 12, h: 9 },
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
        sortBy='Last *',
        sortDesc=true
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
        title='I/O',
        datasource='${datasource}',
        gridPosition={ x: 12, y: 13, w: 12, h: 9 },
        lineWidth=1,
        fillOpacity=8,
        pointSize=5,
        showPoints='auto',
        displayMode='table',
        placement='right',
        tooltip={ mode: 'single', sort: 'none' },
        stackingMode='none',
        decimals=2,
        sortBy='Last *',
        sortDesc=true
      )
      .addCalcs(['lastNotNull'])
      .addTargets([
        $.addTargetSchema(
          expr='rate(smb_smb2_request_total{instance=~"$hostname"}[$__rate_interval])',
          datasource='${datasource}',
          instant=false,
          legendFormat='{{instance}}.{{operation}}',
          range=true,
        ),
      ]),
      $.timeSeriesPanel(
        title='Latencies',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 22, w: 12, h: 9 },
        lineWidth=1,
        fillOpacity=8,
        pointSize=5,
        showPoints='auto',
        unit='µs',
        displayMode='table',
        placement='right',
        tooltip={ mode: 'single', sort: 'none' },
        stackingMode='none',
        decimals=2,
        sortBy='Last *',
        sortDesc=true
      )
      .addCalcs(['lastNotNull'])
      .addTargets([
        $.addTargetSchema(
          expr='rate(smb_smb2_request_duration_microseconds_sum{instance=~"$hostname"}[$__rate_interval])',
          datasource='${datasource}',
          instant=false,
          legendFormat='{{instance}}.{{operation}}',
          range=true,
        ),
      ]),
    ]),
}
