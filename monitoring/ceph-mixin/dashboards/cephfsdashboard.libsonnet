local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'cephfsdashboard.json':
    $.dashboardSchema(
      'Ceph - Filesystem Overview',
      'Ceph CephFS overview for official Ceph Prometheus plugin.',
      '718Bruins',
      'now-6h',
      '30s',
      38,
      $._config.dashboardTags,
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
        1,
        { type: "datasource", uid: "grafana" },
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
      $.addCustomTemplate(
        name='interval',
        query='1m,10m,30m,1h,6h,12h,1d,7d,14d,30d',
        current='$__auto_interval_interval',
        refresh=2,
        label='Interval',
        auto_count=10,
        auto_min='1m',
        options=[
            { selected:true, text: 'auto', value: '$__auto_interval_interval' },
            { selected:false, text: '1m', value: '1m' },
            { selected:false, text: '10m', value: '10m' },
            { selected:false, text: '30m', value: '30m' },
            { selected:false, text: '1h', value: '1h' },
            { selected:false, text: '6h', value: '6h' },
            { selected:false, text: '12h', value: '12h' },
            { selected:false, text: '1d', value: '1d' },
            { selected:false, text: '7d', value: '7d' },
            { selected:false, text: '14d', value: '14d' },
            { selected:false, text: '30d', value: '30d' },
        ],
        auto=true,
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'name',
        { type: "prometheus", uid: "${datasource}" },
        { query: "label_values(ceph_fs_metadata, name)", refId: "StandardVariableQuery" },
        1,
        false,
        0,
        'name',
        '',
        0,
        current={ selected: false, text: "a", value: "a" },
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'mdatapool',
        { type: "prometheus", uid: "${datasource}" },
        { query: "label_values(ceph_fs_metadata{name=~\"$name\"}, metadata_pool)", refId: "StandardVariableQuery" },
        1,
        true,
        0,
        'metadata pool',
        '',
        2,
        current={ selected: false, text: "All", value: "$__all" },
      )
    )
    .addTemplate(
      $.addTemplateSchema(
        'datapool',
        { type: "prometheus", uid: "${datasource}" },
        { query: "label_values(ceph_fs_metadata{name=~\"$name\"}, data_pools)", refId: "StandardVariableQuery" },
        1,
        true,
        0,
        'data pool',
        '',
        2,
        current={ selected: false, text: "All", value: "$__all" },
      )
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
      $.addRowSchema(false,
                     true,
                     'Summary',
                     false) +
      {
        gridPos: { x: 0, y: 0, w: 24, h: 1 }
      },
      $.addStatPanel(
        title='Active MDS',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='none',
        gridPosition={ x: 0, y: 1, w: 3, h: 8 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='count(ceph_mds_metadata)',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata Used',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='bytes',
        gridPosition={ x: 3, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_pool_bytes_used{pool_id=\"$mdatapool\"}',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata Bandwidth Read',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 6, y: 1, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_rd_bytes{pool_id=\"$mdatapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
      )),
      $.addStatPanel(
        title='Data Used',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='bytes',
        gridPosition={ x: 9, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_pool_bytes_used{pool_id=\"$datapool\"}',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Data Bandwidth Read',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 12, y: 1, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_rd_bytes{pool_id=\"$datapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
      )),
      $.addStatPanel(
        title='Client Requests',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 15, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(irate(ceph_mds_server_handle_client_request[$interval]))',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Client Sessions',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 18, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_mds_sessions_session_count)',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='MDS Inodes',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 21, y: 1, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_mds_inodes)',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata Bandwidth Write',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 6, y: 3, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_wr_bytes{pool_id=\"$mdatapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
      )),
      $.addStatPanel(
        title='Data Bandwidth Write',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='Bps',
        gridPosition={ x: 12, y: 3, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_wr_bytes{pool_id=\"$datapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
      )),
      $.addGaugePanel(
        title='Metadata Used',
        datasource={ type: "prometheus", uid: "${datasource}" },
        unit='percentunit',
        max=100,
        min=0,
        gridPosition={ x: 3, y: 5, w: 3, h: 4 },
        pluginVersion='9.4.7',
        maxDataPoints=100,
        interval='1m',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 70 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='(1 - (ceph_pool_max_avail{pool_id=\"$mdatapool\"} / (ceph_pool_bytes_used{pool_id=\"$mdatapool\"} + ceph_pool_max_avail{pool_id=\"$mdatapool\"}))) * 100',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata IOPS Read',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 6, y: 5, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: '#d44a3a', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0 },
        { color: '#508642', value: 0 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_rd{pool_id=\"$mdatapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
      )),
      $.addGaugePanel(
        title='Data Used',
        datasource={ type: "prometheus", uid: "${datasource}" },
        unit='percentunit',
        max=100,
        min=0,
        gridPosition={ x: 9, y: 5, w: 3, h: 4 },
        pluginVersion='9.4.7',
        maxDataPoints=100,
        interval='1m',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 70 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='(1 - (ceph_pool_max_avail{pool_id=\"$datapool\"} / (ceph_pool_bytes_used{pool_id=\"$datapool\"} + ceph_pool_max_avail{pool_id=\"$datapool\"}))) * 100',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Data IOPS Read',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 12, y: 5, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: '#d44a3a', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0 },
        { color: '#508642', value: 0 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_rd{pool_id=\"$datapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
      )),
      $.addStatPanel(
        title='Forward Requests',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 15, y: 5, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(irate(ceph_mds_forward[$interval]))',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Reply Latency',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='ms',
        gridPosition={ x: 18, y: 5, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { index: 0, text: 'N/A' }
          },
          type: 'special'
        },
        {
          options: {
            match: 'nan',
            result: { index: 1, text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(increase(ceph_mds_reply_latency_sum[$interval])) / sum(increase(ceph_mds_reply_latency_count[$interval]))',
        interval='$interval',
        range=true,
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='MDS Caps',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
        gridPosition={ x: 21, y: 5, w: 3, h: 4 },
        colorMode='none',
        graphMode='area',
        pluginVersion='9.4.7',
        interval='1m',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'rgba(50, 172, 45, 0.97)', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0.025 },
        { color: 'rgba(245, 54, 54, 0.9)', value: 0.1 },
      ])
      .addTarget($.addTargetSchema(
        expr='sum(ceph_mds_caps)',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Metadata IOPS Write',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 6, y: 7, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: '#d44a3a', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0 },
        { color: '#508642', value: 0 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_wr{pool_id=\"$mdatapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        instant=true,
      )),
      $.addStatPanel(
        title='Data IOPS Write',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ mode: 'thresholds' },
        unit='iops',
        gridPosition={ x: 12, y: 7, w: 3, h: 2 },
        colorMode='background',
        graphMode='none',
        pluginVersion='9.4.7',
        maxDataPoints=100,
        thresholdsMode='absolute',
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: '#d44a3a', value: null },
        { color: 'rgba(237, 129, 40, 0.89)', value: 0 },
        { color: '#508642', value: 0 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_pool_wr{pool_id=\"$datapool\"}[$interval])',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        instant=true,
      )),
      $.addRowSchema( collapse=false, showTitle=true, title='MDS', collapsed=false) + { gridPos: { x: 0, y: 9, w: 24, h: 1 } },
      $.timeSeriesPanel(
          title='Client Requests',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 0, y: 10 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
        {
          options: {
            pattern: '(?<=\\.)(.*?)(?=\\.)',
            result: { index: 0, text: '$1' }
          },
          type: 'regex'
        }
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_mds_server_handle_client_request[$interval])',
        datasource={ type: "prometheus", uid: "${datasource}" },
        range=true,
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Forward Requests',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 8, y: 10 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_mds_forward[$interval])',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Slave Requests',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 16, y: 10 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_mds_server_handle_slave_request[$interval])',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Session Count',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 12, x: 0, y: 17 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_sessions_session_count',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Reply Latency',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 12, x: 12, y: 17 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='ms',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='increase(ceph_mds_reply_latency_sum[$interval]) / increase(ceph_mds_reply_latency_count[$interval])',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.addRowSchema( collapse=false, showTitle=true, title='Log', collapsed=false) + { gridPos: { x: 0, y: 24, w: 24, h: 1 } },
      $.timeSeriesPanel(
          title='Log Submit',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 0, y: 25 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='irate(ceph_mds_log_evadd{}[$interval])',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Log Events',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 8, y: 25 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_log_ev',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Log Segments',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 16, y: 25 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green' },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_log_seg',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.addRowSchema(false, true, 'Alerts', collapsed=true)
      .addPanels([
        $.addAlertListPanel(
          title='Active Alerts',
          datasource={
            type: 'datasource',
            uid: 'grafana',
          },
          gridPosition={ x: 0, y: 33, w: 24, h: 8 },
        alertInstanceLabelFilter='',
        alertName='CephFilesystem',
        dashboardAlerts=false,
        groupBy=[],
        groupMode='default',
        maxItems=20,
        sortOrder=3,
        stateFilter={ 'error': true, firing: true, noData: false, normal: false, pending: true },
      ),
      ])
      + { gridPos: { x: 0, y: 32, w: 24, h: 1 } },
      $.addRowSchema( collapse=false, showTitle=true, title='Memory', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          title='Inodes',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 0, y: 42 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_mem_ino',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Exported Inodes',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 8, y: 42 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_exported_inodes',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Imported Inodes',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 16, y: 42 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_imported_inodes',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Dentries',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 0, y: 49 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_mem_dn',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      $.timeSeriesPanel(
          title='Caps',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 8, x: 8, y: 49 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=1,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=false,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_mds_caps',
        datasource={ type: "prometheus", uid: "${datasource}" },
        legendFormat='{{ ceph_daemon }}',
      )),
      ])
      + { gridPos: { x: 0, y: 41, w: 24, h: 1 } },
      $.addRowSchema( collapse=false, showTitle=true, title='Metadata Pool', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          title='Pool Storage',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 14, x: 0, y: 57 },
          fillOpacity=40,
          pointSize=5,
          lineWidth=0,
          showPoints='never',
          unit='bytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='right',
          showLegend=true,
          interval='$interval',
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addCalcs(['mean', 'lastNotNull', 'max'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byRegexp', options: '/^Total.*$/' },
            properties: [
              {
                id: 'custom.fillOpacity',
                value: 0,
              },
              {
                id: 'custom.lineWidth',
                value: 4,
              },
              {
                id: 'custom.stacking',
                value: { group: false, mode: 'normal' },
              },
            ],
          },
          {
            matcher: { id: 'byRegexp', options: '/^Raw.*$/' },
            properties: [
              {
                id: 'color',
                value: { fixedColor: '#BF1B00', mode: 'fixed' },
              },
              {
                id: 'custom.fillOpacity',
                value: 0,
              },
              {
                id: 'custom.lineWidth',
                value: 4,
              },
            ],
          },
        ]
      )
      .addTargets(
        [
          $.addTargetSchema(
            expr='ceph_pool_max_avail{pool_id=~\"^$mdatapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Available',
            metric='ceph_pool_available_bytes',
            step=60,
            range=true,
          ),
          $.addTargetSchema(
            expr='ceph_pool_bytes_used{pool_id=~\"^$mdatapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Used',
            metric='ceph_pool',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_bytes_used{pool_id=~\"^$mdatapool$\"} + ceph_pool_max_avail{pool_id=~\"^$mdatapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Total',
            metric='ceph_pool',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_raw_bytes_used{pool_id=~\"^$mdatapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Raw Used',
            metric='ceph_pool',
            step=60,
          ),
        ]
      ),
      $.timeSeriesPanel(
          title='Objects in Pool',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 6, x: 14, y: 57 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addCalcs([])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='ceph_pool_objects{pool_id=~\"$mdatapool\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Objects',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_dirty{pool_id=~\"$mdatapool\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Dirty Objects',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_quota_objects{pool_id=~\"$mdatapool\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Quota Objects',
          ),
        ]
      ),
      $.addGaugePanel(
        title='Usage',
        datasource={ type: "prometheus", uid: "${datasource}" },
        unit='percentunit',
        max=1,
        min=0,
        gridPosition={ x: 20, y: 57, w: 4, h: 7 },
        pluginVersion='9.4.7',
        maxDataPoints=100,
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_pool_bytes_used{pool_id=\"$mdatapool\"} / (ceph_pool_bytes_used{pool_id=\"$mdatapool\"} + ceph_pool_max_avail{pool_id=\"$mdatapool\"})',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.timeSeriesPanel(
          title='IOPS',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 12, x: 0, y: 64 },
          axisLabel='IOPS',
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='none',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          interval='$interval',
          min=0,
        ).addMappings([
      ])
      .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='irate(ceph_pool_rd{pool_id=~\"$mdatapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Read',
            step=60,
          ),
          $.addTargetSchema(
            expr='irate(ceph_pool_wr{pool_id=~\"$mdatapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Write',
            step=60,
          ),
        ]
      ),
      $.timeSeriesPanel(
          title='Throughput',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 12, x: 12, y: 64 },
          axisLabel='IOPS',
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='decbytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          interval='$interval',
          min=0,
        ).addMappings([
      ])
      .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='irate(ceph_pool_rd_bytes{pool_id=~\"$mdatapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Read Bytes',
            step=60,
          ),
          $.addTargetSchema(
            expr='irate(ceph_pool_wr_bytes{pool_id=~\"$mdatapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Write Bytes',
            step=60,
          ),
        ]
      ),
      ])
      + { gridPos: { x: 0, y: 56, w: 24, h: 1 } },
      $.addRowSchema( collapse=false, showTitle=true, title='Data Pool', collapsed=true)
      .addPanels([
        $.timeSeriesPanel(
          title='Pool Storage',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 14, x: 0, y: 72 },
          fillOpacity=40,
          pointSize=5,
          lineWidth=0,
          showPoints='never',
          unit='bytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='right',
          showLegend=true,
          interval='$interval',
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addCalcs(['mean', 'lastNotNull', 'max'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides(
        [
          {
            matcher: { id: 'byRegexp', options: '/^Total.*$/' },
            properties: [
              {
                id: 'custom.fillOpacity',
                value: 0,
              },
              {
                id: 'custom.lineWidth',
                value: 4,
              },
              {
                id: 'custom.stacking',
                value: { group: false, mode: 'normal' },
              },
            ],
          },
          {
            matcher: { id: 'byRegexp', options: '/^Raw.*$/' },
            properties: [
              {
                id: 'color',
                value: { fixedColor: '#BF1B00', mode: 'fixed' },
              },
              {
                id: 'custom.fillOpacity',
                value: 0,
              },
              {
                id: 'custom.lineWidth',
                value: 4,
              },
            ],
          },
        ]
      )
      .addTargets(
        [
          $.addTargetSchema(
            expr='ceph_pool_max_avail{pool_id=~\"^$datapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Available',
            metric='ceph_pool_available_bytes',
            step=60,
            range=true,
          ),
          $.addTargetSchema(
            expr='ceph_pool_bytes_used{pool_id=~\"^$datapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Used',
            metric='ceph_pool',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_bytes_used{pool_id=~\"^$datapool$\"} + ceph_pool_max_avail{pool_id=~\"^$mdatapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Total',
            metric='ceph_pool',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_raw_bytes_used{pool_id=~\"^$datapool$\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Raw Used',
            metric='ceph_pool',
            step=60,
          ),
        ]
      ),
      $.timeSeriesPanel(
          title='Objects in Pool',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 6, x: 14, y: 72 },
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='short',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='none',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='list',
          placement='bottom',
          showLegend=true,
          min=0,
          pluginVersion='9.4.7',
        ).addMappings([
      ])
      .addCalcs([])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='ceph_pool_objects{pool_id=~\"$datapool\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Objects',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_dirty{pool_id=~\"$datapool\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Dirty Objects',
            step=60,
          ),
          $.addTargetSchema(
            expr='ceph_pool_quota_objects{pool_id=~\"$datapool\"}',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Quota Objects',
          ),
        ]
      ),
      $.addGaugePanel(
        title='Usage',
        datasource={ type: "prometheus", uid: "${datasource}" },
        unit='percentunit',
        max=1,
        min=0,
        gridPosition={ x: 20, y: 72, w: 4, h: 7 },
        pluginVersion='9.4.7',
        maxDataPoints=100,
      ).addMappings([
        {
          options: {
            match: 'null',
            result: { text: 'N/A' }
          },
          type: 'special'
        }
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addTarget($.addTargetSchema(
        expr='ceph_pool_bytes_used{pool_id=\"$datapool\"} / (ceph_pool_bytes_used{pool_id=\"$datapool\"} + ceph_pool_max_avail{pool_id=\"$datapool\"})',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.timeSeriesPanel(
          title='IOPS',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 12, x: 0, y: 79 },
          axisLabel='IOPS',
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='none',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          interval='$interval',
          min=0,
        ).addMappings([
      ])
      .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='irate(ceph_pool_rd{pool_id=~\"$datapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Read',
            step=60,
          ),
          $.addTargetSchema(
            expr='irate(ceph_pool_wr{pool_id=~\"$datapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Write',
            step=60,
          ),
        ]
      ),
      $.timeSeriesPanel(
          title='Throughput',
          datasource={ type: "prometheus", uid: "${datasource}" },
          gridPosition={ h: 7, w: 12, x: 12, y: 79 },
          axisLabel='IOPS',
          fillOpacity=10,
          pointSize=5,
          lineWidth=2,
          showPoints='never',
          unit='decbytes',
          tooltip={ mode: 'multi', sort: 'none' },
          stackingGroup='A',
          stackingMode='normal',
          spanNulls=true,
          decimals=null,
          thresholdsMode='absolute',
          displayMode='table',
          placement='bottom',
          showLegend=true,
          pluginVersion='9.4.7',
          interval='$interval',
          min=0,
        ).addMappings([
      ])
      .addCalcs(['mean', 'lastNotNull', 'max', 'min'])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addOverrides([
      ])
      .addThresholds([
        { color: 'green', value: null },
        { color: 'red', value: 80 },
      ])
      .addTargets(
        [
          $.addTargetSchema(
            expr='irate(ceph_pool_rd_bytes{pool_id=~\"$datapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Read Bytes',
            step=60,
          ),
          $.addTargetSchema(
            expr='irate(ceph_pool_wr_bytes{pool_id=~\"$datapool\"}[$interval])',
            datasource={ type: "prometheus", uid: "${datasource}" },
            interval='$interval',
            legendFormat='Write Bytes',
            step=60,
          ),
        ]
      ),
      ])
      + { gridPos: { x: 0, y: 71, w: 24, h: 1 } },
    ]),
    }