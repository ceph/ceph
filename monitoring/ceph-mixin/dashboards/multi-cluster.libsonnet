local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'multi-cluster-overview.json':
    $.dashboardSchema(
      'Ceph - Multi-cluster',
      '',
      'BnxelG7Sz',
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
      g.template.datasource('DS_PROMETHEUS', 'prometheus', 'default', label='Data Source')
    )

    .addTemplate(
      $.addTemplateSchema('Cluster',
                          '$DS_PROMETHEUS',
                          'label_values(ceph_health_status, cluster)',
                          2,
                          true,
                          0,
                          null,
                          '',
                          current='All')
    )
    .addPanels([
      $.addRowSchema(false, true, 'Clusters') + { gridPos: { x: 0, y: 1, w: 24, h: 1 } },
      $.addStatPanel(
        title='Status',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 0, y: 2, w: 5, h: 7 },
        graphMode='none',
        colorMode='value',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'text', value: null },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byName', options: 'Warning' },
            properties: [
              {
                id: 'thresholds',
                value: { mode: 'absolute', steps: [{ color: 'text', value: null }, { color: 'semi-dark-yellow', value: 1 }] },
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Error' },
            properties: [
              {
                id: 'thresholds',
                value: { mode: 'absolute', steps: [{ color: 'text', value: null }, { color: 'semi-dark-red', value: 1 }] },
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Healthy' },
            properties: [
              {
                id: 'thresholds',
                value: { mode: 'absolute', steps: [{ color: 'text', value: null }, { color: 'semi-dark-green', value: 1 }] },
              },
            ],
          },
        ]
      )
      .addTargets([
        $.addTargetSchema(
          expr='count(ceph_health_status==0) or vector(0)',
          datasource='${DS_PROMETHEUS}',
          legendFormat='Healthy',
        ),
        $.addTargetSchema(
          expr='count(ceph_health_status==1)',
          datasource='${DS_PROMETHEUS}',
          legendFormat='Warning'
        ),
        $.addTargetSchema(
          expr='count(ceph_health_status==2)',
          datasource='${DS_PROMETHEUS}',
          legendFormat='Error'
        ),
      ]),

      $.addTableExtended(
        datasource='${DS_PROMETHEUS}',
        title='Details',
        gridPosition={ h: 7, w: 19, x: 5, y: 2 },
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
        custom={ align: 'left', cellOptions: { type: 'color-text' }, filterable: false, inspect: false },
        thresholds={
          mode: 'absolute',
          steps: [
            { color: 'text' },
          ],
        },
        overrides=[
          {
            matcher: { id: 'byName', options: 'Value #A' },
            properties: [
              { id: 'mappings', value: [{ options: { '0': { color: 'semi-dark-green', index: 2, text: 'Healthy' }, '1': { color: 'semi-dark-yellow', index: 0, text: 'Warning' }, '2': { color: 'semi-dark-red', index: 1, text: 'Error' } }, type: 'value' }] },
            ],
          },
          {
            matcher: { id: 'byName', options: 'IOPS' },
            properties: [
              { id: 'unit', value: 'ops' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #E' },
            properties: [
              { id: 'unit', value: 'bytes' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Capacity Used' },
            properties: [
              { id: 'unit', value: 'bytes' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Cluster' },
            properties: [
              { id: 'links', value: [{ title: '', url: '/d/GQ3MHvnIz/ceph-cluster-new?var-cluster=${__data.fields.Cluster}&${DS_PROMETHEUS:queryparam}' }] },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Alerts' },
            properties: [
              { id: 'mappings', value: [{ options: { match: null, result: { index: 0, text: '0' } }, type: 'special' }] },
            ],
          },
        ],
        pluginVersion='9.4.7'
      )
      .addTransformations([
        {
          id: 'joinByField',
          options: { byField: 'cluster', mode: 'outer' },
        },
        {
          id: 'organize',
          options: {
            excludeByName: {
              'Time 1': true,
              'Time 2': true,
              'Time 3': true,
              'Time 4': true,
              'Time 5': true,
              'Time 6': true,
              'Value #B': true,
              '__name__ 1': true,
              '__name__ 2': true,
              '__name__ 3': true,
              ceph_daemon: true,
              device_class: true,
              hostname: true,
              'instance 1': true,
              'instance 2': true,
              'instance 3': true,
              'job 1': true,
              'job 2': true,
              'job 3': true,
              'replica 1': true,
              'replica 2': true,
              'replica 3': true,
            },
            indexByName: {
              'Time 1': 8,
              'Time 2': 13,
              'Time 3': 21,
              'Time 4': 7,
              'Time 5': 22,
              'Time 6': 23,
              'Value #A': 1,
              'Value #B': 20,
              'Value #C': 3,
              'Value #D': 4,
              'Value #E': 5,
              'Value #F': 6,
              '__name__ 1': 9,
              '__name__ 2': 14,
              '__name__ 3': 24,
              ceph_daemon: 15,
              ceph_version: 2,
              cluster: 0,
              device_class: 25,
              hostname: 16,
              'instance 1': 10,
              'instance 2': 17,
              'instance 3': 26,
              'job 1': 11,
              'job 2': 18,
              'job 3': 27,
              'replica 1': 12,
              'replica 2': 19,
              'replica 3': 28,
            },
            renameByName: {
              'Value #A': 'Status',
              'Value #C': 'Alerts',
              'Value #D': 'IOPS',
              'Value #E': 'Throughput',
              'Value #F': 'Capacity Used',
              ceph_version: 'Version',
              cluster: 'Cluster',
            },
          },
        },
      ]).addTargets([
        $.addTargetSchema(
          expr='ceph_health_status',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_mgr_metadata',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='count(ALERTS{alertstate="firing", cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='sum by (cluster) (irate(ceph_pool_wr[$__interval]))  \n+ sum by (cluster) (irate(ceph_pool_rd[$__interval])) ',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='sum by (cluster) (irate(ceph_pool_rd_bytes[$__interval]))\n+ sum by (cluster) (irate(ceph_pool_wr_bytes[$__interval])) ',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
        $.addTargetSchema(
          expr='ceph_cluster_by_class_total_used_bytes',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
      ]),


      $.addRowSchema(false, true, 'Overview') + { gridPos: { x: 0, y: 9, w: 24, h: 1 } },
      $.addStatPanel(
        title='Cluster Count',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 0, y: 10, w: 3, h: 4 },
        graphMode='none',
        colorMode='value',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'text', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(ceph_health_status{cluster=~"$Cluster"}) or vector(0)',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
      ]),

      $.addGaugePanel(
        title='Capacity Used',
        gridPosition={ h: 8, w: 4, x: 3, y: 10 },
        unit='percentunit',
        max=1,
        min=0,
        interval='1m',
        pluginVersion='9.4.7'
      )
      .addThresholds([
        { color: 'green', value: null },
        { color: 'semi-dark-yellow', value: 0.75 },
        { color: 'red', value: 0.85 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_cluster_total_used_bytes{cluster=~"$Cluster"}) / sum(ceph_cluster_total_bytes{cluster=~"$Cluster"})',
        instant=true,
        legendFormat='Used',
        datasource='${DS_PROMETHEUS}',
      )),

      $.addStatPanel(
        title='Total Capacity',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 7, y: 10, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        unit='bytes',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_cluster_total_bytes{cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=false,
          interval='',
          legendFormat='__auto',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='OSDs',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 10, y: 10, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        unit='none',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(ceph_osd_metadata{cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=false,
          interval='',
          legendFormat='__auto',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Hosts',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 13, y: 10, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        unit='none',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(sum by (hostname) (ceph_osd_metadata{cluster=~"$Cluster"}))',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=false,
          interval='',
          legendFormat='__auto',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Client IOPS',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 16, y: 10, w: 4, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        unit='ops',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(irate(ceph_pool_wr{cluster=~"$Cluster"}[$__interval]))',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          instant=false,
          legendFormat='Write',
          range=true,
        ),
        $.addTargetSchema(
          expr='sum(irate(ceph_pool_rd{cluster=~"$Cluster"}[$__interval]))',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          legendFormat='Read',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='OSD Latencies',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 20, y: 10, w: 4, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        unit='ms',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='avg(ceph_osd_apply_latency_ms{cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          instant=false,
          legendFormat='Apply',
          range=true,
        ),
        $.addTargetSchema(
          expr='avg(ceph_osd_commit_latency_ms{cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          legendFormat='Commit',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Alert Count',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 0, y: 14, w: 3, h: 4 },
        graphMode='none',
        colorMode='value',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'text', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(ALERTS{alertstate="firing", cluster=~"$Cluster"}) or vector(0)',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='__auto',
          range=false,
        ),
      ]),

      $.addStatPanel(
        title='Total Used',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 7, y: 14, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        unit='bytes',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_cluster_total_used_bytes{cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=false,
          interval='',
          legendFormat='__auto',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Capacity Prediction',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 10, y: 14, w: 3, h: 4 },
        graphMode='none',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        unit='s',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='predict_linear(avg(increase(ceph_cluster_total_used_bytes{cluster=~"${Cluster}"}[1d]))[7d:1h],120)',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          legendFormat='__auto',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Pools',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 13, y: 14, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        unit='none',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(ceph_pool_metadata{cluster=~"$Cluster"})',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=false,
          interval='',
          legendFormat='__auto',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Client Bandwidth',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 16, y: 14, w: 4, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        unit='binBps',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(irate(ceph_pool_rd_bytes{cluster=~"$Cluster"}[$__interval]))',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          instant=false,
          legendFormat='Write',
          range=true,
        ),
        $.addTargetSchema(
          expr='sum(irate(ceph_pool_wr_bytes{cluster=~"$Cluster"}[$__interval]))',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          legendFormat='Read',
          range=true,
        ),
      ]),

      $.addStatPanel(
        title='Recovery Rate',
        datasource='${DS_PROMETHEUS}',
        gridPosition={ x: 20, y: 14, w: 4, h: 4 },
        graphMode='area',
        colorMode='none',
        orientation='auto',
        justifyMode='center',
        thresholdsMode='absolute',
        unit='binBps',
        pluginVersion='9.4.7',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(irate(ceph_osd_recovery_ops{cluster=~"$Cluster"}[$__interval]))',
          datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
          hide=false,
          exemplar=false,
          instant=false,
          legendFormat='Write',
          range=true,
        ),
      ]),


      $.addRowSchema(false, true, 'Alerts', collapsed=true)
      .addPanels([
        $.addStatPanel(
          title='Status',
          datasource='${DS_PROMETHEUS}',
          gridPosition={ x: 0, y: 19, w: 5, h: 7 },
          graphMode='area',
          colorMode='value',
          orientation='auto',
          justifyMode='center',
          thresholdsMode='absolute',
          pluginVersion='9.4.7',
        ).addThresholds([
          { color: 'text', value: null },
        ])
        .addOverrides(
          [
            {
              matcher: { id: 'byName', options: 'Critical' },
              properties: [
                {
                  id: 'thresholds',
                  value: { mode: 'absolute', steps: [{ color: 'text', value: null }, { color: 'semi-dark-red', value: 1 }] },
                },
              ],
            },
            {
              matcher: { id: 'byName', options: 'Warning' },
              properties: [
                {
                  id: 'thresholds',
                  value: { mode: 'absolute', steps: [{ color: 'text', value: null }, { color: 'semi-dark-yellow', value: 1 }] },
                },
              ],
            },
          ]
        )
        .addTargets([
          $.addTargetSchema(
            expr='count(ALERTS{alertstate="firing",severity="critical", cluster=~"$Cluster"}) OR vector(0)',
            datasource='${DS_PROMETHEUS}',
            legendFormat='Critical',
            instant=true,
            range=false
          ),
          $.addTargetSchema(
            expr='count(ALERTS{alertstate="firing",severity="warning", cluster=~"$Cluster"}) OR vector(0)',
            datasource='${DS_PROMETHEUS}',
            legendFormat='Warning',
            instant=true,
            range=false
          ),
        ]),


        $.addTableExtended(
          datasource='${DS_PROMETHEUS}',
          title='Alerts',
          gridPosition={ h: 7, w: 19, x: 5, y: 19 },
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
            sortBy: [{ desc: false, displayName: 'Severity' }],
          },
          custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: true, inspect: false },
          thresholds={
            mode: 'absolute',
            steps: [
              { color: 'green' },
              { color: 'red', value: 80 },
            ],
          },
          pluginVersion='9.4.7'
        )
        .addTransformations([
          {
            id: 'joinByField',
            options: { byField: 'cluster', mode: 'outer' },
          },
          {
            id: 'organize',
            options: {
              excludeByName: {
                Time: true,
                Value: true,
                __name__: true,
                instance: true,
                job: true,
                oid: true,
                replica: true,
                type: true,
              },
              indexByName: {
                Time: 0,
                Value: 9,
                __name__: 1,
                alertname: 2,
                alertstate: 4,
                cluster: 3,
                instance: 6,
                job: 7,
                severity: 5,
                type: 8,
              },
              renameByName: {
                alertname: 'Name',
                alertstate: 'State',
                cluster: 'Cluster',
                severity: 'Severity',
              },
            },
          },
        ]).addTargets([
          $.addTargetSchema(
            expr='ALERTS{alertstate="firing", cluster=~"$Cluster"}',
            datasource={ type: 'prometheus', uid: '${DS_PROMETHEUS}' },
            format='table',
            hide=false,
            exemplar=false,
            instant=true,
            interval='',
            legendFormat='__auto',
            range=false,
          ),
        ]),

        $.addAlertListPanel(
          title='Alerts(Grouped)',
          datasource={
            type: 'datasource',
            uid: 'grafana',
          },
          gridPosition={ h: 8, w: 24, x: 0, y: 26 },
          alertName='',
          dashboardAlerts=false,
          groupBy=[],
          groupMode='default',
          maxItems=20,
          sortOrder=1,
          stateFilter={
            'error': true,
            firing: true,
            noData: false,
            normal: false,
            pending: true,
          },
        ),
      ]) + { gridPos: { x: 0, y: 18, w: 24, h: 1 } },

      $.addRowSchema(false, true, 'Cluster Stats', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Top 5 - Capacity Utilization(%)',
          datasource='${DS_PROMETHEUS}',
          gridPosition={ h: 7, w: 8, x: 0, y: 30 },
          fillOpacity=0,
          pointSize=5,
          showPoints='auto',
          unit='percentunit',
          displayMode='table',
          showLegend=true,
          placement='bottom',
          tooltip={ mode: 'multi', sort: 'desc' },
          stackingMode='none',
          spanNulls=false,
          decimals=2,
          thresholdsMode='percentage',
          sortBy='Last',
          sortDesc=true
        )
        .addCalcs(['last'])
        .addThresholds([
          { color: 'green' },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='topk(5, ceph_cluster_total_used_bytes/ceph_cluster_total_bytes)',
              datasource='${DS_PROMETHEUS}',
              instant=false,
              legendFormat='{{cluster}}',
              step=300,
              range=true,
            ),
          ]
        ),


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Top 5 - Cluster IOPS',
          datasource='${DS_PROMETHEUS}',
          gridPosition={ h: 7, w: 8, x: 8, y: 30 },
          fillOpacity=0,
          pointSize=5,
          showPoints='auto',
          unit='ops',
          displayMode='table',
          showLegend=true,
          placement='bottom',
          tooltip={ mode: 'multi', sort: 'desc' },
          stackingMode='none',
          spanNulls=false,
          decimals=2,
          thresholdsMode='percentage',
          sortBy='Last',
          sortDesc=true
        )
        .addCalcs(['last'])
        .addThresholds([
          { color: 'green' },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='topk(10, sum by (cluster) (irate(ceph_osd_op_w[$__interval]))  \n+ sum by (cluster) (irate(ceph_osd_op_r[$__interval])) )',
              datasource='${DS_PROMETHEUS}',
              instant=false,
              legendFormat='{{cluster}}',
              step=300,
              range=true,
            ),
          ]
        ),


        $.timeSeriesPanel(
          lineInterpolation='linear',
          lineWidth=1,
          drawStyle='line',
          axisPlacement='auto',
          title='Top 10 - Capacity Utilization(%) by Pool',
          datasource='${DS_PROMETHEUS}',
          gridPosition={ h: 7, w: 8, x: 16, y: 30 },
          fillOpacity=0,
          pointSize=5,
          showPoints='auto',
          unit='percentunit',
          displayMode='table',
          showLegend=true,
          placement='bottom',
          tooltip={ mode: 'multi', sort: 'desc' },
          stackingMode='none',
          spanNulls=false,
          decimals=2,
          thresholdsMode='absolute',
          sortBy='Last',
          sortDesc=true
        )
        .addCalcs(['last'])
        .addThresholds([
          { color: 'green' },
        ])
        .addTargets(
          [
            $.addTargetSchema(
              expr='topk(10, ceph_pool_bytes_used{cluster=~"$Cluster"}/ceph_pool_max_avail{cluster=~"$Cluster"} * on(pool_id, cluster) group_left(instance, name) ceph_pool_metadata{cluster=~"$Cluster"})',
              datasource='${DS_PROMETHEUS}',
              instant=false,
              legendFormat='{{cluster}} - {{name}}',
              step=300,
              range=true,
            ),
          ]
        ),
      ]) + { gridPos: { x: 0, y: 29, w: 24, h: 1 } },
    ]),
}
