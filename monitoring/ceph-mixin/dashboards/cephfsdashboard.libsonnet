local g = import '/home/piyushagarwal/Desktop/ceph_clone/ceph/monitoring/ceph-mixin/grafonnet-lib/grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'cephfsdashboard.json':
    $.dashboardSchema(
      'Ceph - CephFS',
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
            match: null,
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
        title='MetaData Used',
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
            match: null,
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
        title='MetaData Read',
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
            match: null,
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
            match: null,
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
        title='Data Read',
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
            match: null,
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
            match: null,
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
            match: null,
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
            match: null,
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
            match: null,
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
        title='MetaData Write',
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
            match: null,
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
        title='Data Write',
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
            match: null,
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
        title='MetaData Available',
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
            match: null,
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
        expr='ceph_pool_max_avail{pool_id=\"$mdatapool\"} / (ceph_pool_bytes_used{pool_id=\"$mdatapool\"} + ceph_pool_max_avail{pool_id=\"$mdatapool\"})',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='MetaData Read',
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
            match: null,
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
        title='Data Available',
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
            match: null,
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
        expr='ceph_pool_max_avail{pool_id=\"$datapool\"} / (ceph_pool_bytes_used{pool_id=\"$datapool\"} + ceph_pool_max_avail{pool_id=\"$datapool\"})',
        interval='$interval',
        datasource={ type: "prometheus", uid: "${datasource}" },
        step=60,
      )),
      $.addStatPanel(
        title='Data Read',
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
            match: null,
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
            match: null,
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
        title='Reply Latency(ms)',
        datasource={ type: "prometheus", uid: "${datasource}" },
        color={ fixedColor: 'rgb(31, 120, 193)', mode: 'fixed' },
        unit='short',
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
            match: null,
            result: { index: 0, text: 'N/A' }
          },
          type: 'special'
        },
        {
          options: {
            match: null,
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
        expr='sum(increase(ceph_mds_reply_latency_sum[$interval])) * 1000 / sum(increase(ceph_mds_reply_latency_count[$interval]))',
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
            match: null,
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
    ]),
    }