local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'cephfs-overview.json':
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
        datasource={ type: "prometheus", uid: "${datasource}" },
        query={ query: "label_values(ceph_fs_metadata, name)", refId: "StandardVariableQuery" },
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
        datasource={ type: "prometheus", uid: "${datasource}" },
        query={ query: "label_values(ceph_fs_metadata{name=~\"$name\"}, metadata_pool)", refId: "StandardVariableQuery" },
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
        datasource={ type: "prometheus", uid: "${datasource}" },
        query={ query: "label_values(ceph_fs_metadata{name=~\"$name\"}, data_pools)", refId: "StandardVariableQuery" },
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
    ])