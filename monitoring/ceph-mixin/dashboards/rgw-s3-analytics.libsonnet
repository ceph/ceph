local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  // Usage metrics have no instance_id/ceph_daemon label, but they carry the RGW zone id in the
  // 'zone' label. ceph_rgw_metadata exposes the same id as 'zone_id', so the $rgw_instance filter
  // is applied by restricting usage series to the zones of the selected daemons.
  rgwS3SelectedZones:: 'label_replace(ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s}, "zone", "$1", "zone_id", "(.+)")' % $.matchers(),

  // One selected RGW daemon per zone (keeps the ceph_daemon label). Used to tell the Users & Buckets
  // dashboard which $rgw_instance to select when a user/bucket link is clicked.
  rgwS3ZoneDaemon:: 'topk by (zone) (1, %s)' % $.rgwS3SelectedZones,

  // The ceph_daemon field (RGW instance) is only used by the user/bucket links to select
  // $rgw_instance on the Users & Buckets dashboard. It stays in the data frame but is hidden in the table.
  rgwS3HideInstance:: { matcher: { id: 'byName', options: 'ceph_daemon' }, properties: [{ id: 'custom.hidden', value: true }] },

  // Per-bucket performance counter summed per bucket and owner, restricted to the selected
  // $rgw_instance. The owner comes from the usage metrics (the only bucket->user mapping) and the
  // counters are matched on bucket name, tenant and the zone of the serving RGW daemon
  // (instance_id -> ceph_rgw_metadata.zone_id), so same-named buckets of different users or zones
  // stay separate rows. Buckets without usage series are kept with owner "All".
  rgwS3BucketPerfByOwner:: function(metric)
    local withZone = '(%s * on (instance_id) group_left (zone, ceph_daemon) %s)' % [metric, $.rgwS3SelectedZones];
    'sum by (bucket, owner, ceph_daemon) (%(w)s * on (bucket, tenant, zone) group_left (owner) group by (bucket, tenant, zone, owner) (ceph_rgw_bucket_usage_bytes)) or label_replace(sum by (bucket, ceph_daemon) (%(w)s unless on (bucket, tenant, zone) ceph_rgw_bucket_usage_bytes), "owner", "All", "bucket", ".*")' % { w: withZone },

  // Top-5 table over a usage metric, restricted to the zones of the selected $rgw_instance.
  //   entity: 'user' (grouped by owner) or 'bucket' (grouped by bucket, owner, zone)
  //   metric: usage metric name; valueName: displayed value column; unit: grafana unit
  rgwS3TopTable:: function(title, description, gridPosition, entity, metric, valueName, unit)
    local isUser = entity == 'user';
    local groupBy = if isUser then 'owner, ceph_daemon' else 'bucket, owner, ceph_daemon';
    local userLink = [{
      title: 'Open ${__value.raw}',
      url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
      targetBlank: false,
    }];
    local bucketLink = [{
      title: 'Open ${__value.raw}',
      url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-bucket=${__value.raw}&var-user=${__data.fields.owner}&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
      targetBlank: false,
    }];
    local valueOverride(name) = {
      matcher: { id: 'byName', options: name },
      properties: [
        { id: 'displayName', value: valueName },
        { id: 'unit', value: unit },
      ],
    };
    $.addTableExtended(
      datasource='${datasource}',
      title=title,
      gridPosition=gridPosition,
      description=description,
      options={
        footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
        frameIndex: 0,
        showHeader: true,
        sortBy: [{ desc: true, displayName: valueName }],
      },
      custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
      thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
      overrides=(
        if isUser then [
          {
            matcher: { id: 'byName', options: 'owner' },
            properties: [{ id: 'displayName', value: 'User' }, { id: 'links', value: userLink }],
          },
          { matcher: { id: 'byName', options: 'User' }, properties: [{ id: 'links', value: userLink }] },
        ] else [
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [{ id: 'displayName', value: 'Bucket' }, { id: 'links', value: bucketLink }],
          },
          { matcher: { id: 'byName', options: 'Bucket' }, properties: [{ id: 'links', value: bucketLink }] },
          {
            matcher: { id: 'byName', options: 'owner' },
            properties: [{ id: 'displayName', value: 'User' }, { id: 'links', value: userLink }],
          },
          { matcher: { id: 'byName', options: 'User' }, properties: [{ id: 'links', value: userLink }] },
        ]
      ) + [
        $.rgwS3HideInstance,
        valueOverride('Value'),
        valueOverride('Value #A'),
        { matcher: { id: 'byName', options: valueName }, properties: [{ id: 'unit', value: unit }] },
      ],
      pluginVersion='9.4.7'
    ).addTransformations([
      {
        id: 'organize',
        options: {
          // ceph_daemon (RGW Instance) is kept: it identifies the zone's RGW daemon and is passed
          // to the Users & Buckets dashboard by the links.
          excludeByName: { Time: true, time: true, __name__: true },
          indexByName: if isUser then { owner: 0, User: 0, Value: 1, ceph_daemon: 2 } else { bucket: 0, Bucket: 0, owner: 1, User: 1, Value: 2, ceph_daemon: 3 },
          renameByName: {},
        },
      },
    ]).addTargets([
      $.addTargetSchema(
        expr='topk(5, sum by (%s) (%s * on (zone) group_left (ceph_daemon) %s))' % [groupBy, metric, $.rgwS3ZoneDaemon],
        datasource={ type: 'prometheus', uid: '${datasource}' },
        format='table',
        hide=false,
        exemplar=false,
        instant=true,
        interval='',
        legendFormat='',
        range=false,
      ),
    ]),

  'rgw-s3-analytics.json':
    $.dashboardSchema(
      'Ceph Object - S3 Analytics - Overview',
      '',
      'BnxelG7Sz',
      'now-1h',
      '1m',
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
    .addTemplate(
      g.template.datasource('datasource', 'prometheus', 'default', label='Data Source')
    )
    .addTemplate(
      $.addClusterTemplate()
    )
    .addTemplate(
      $.addTemplateSchema('rgw_instance',
                          '$datasource',
                          'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          2,
                          true,
                          0,
                          null,
                          '')
    )
    .addTemplate(
      g.template.adhoc('Filters', '$datasource', 'filters', 0)
    )

    .addPanels([
      // ── Row: Overview ─────────────────────────────────────────────────────
      $.addRowSchema(false, true, 'Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },

      // Summary stats (single row): Buckets, Users, Used Bytes, Object Count, Avg Object Size,
      // PUT / GET Bytes (stacked), PUT / GET Ops (stacked), Compression Ratio
      $.addStatPanel(
        title='Buckets',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Total number of buckets (usage metric).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(ceph_rgw_bucket_usage_bytes and on (zone) %s)' % $.rgwS3SelectedZones,
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Users',
        datasource='${datasource}',
        gridPosition={ x: 3, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='none',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Total number of users that own at least one bucket (usage metric).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='count(count by (owner) (ceph_rgw_bucket_usage_bytes and on (zone) %s))' % $.rgwS3SelectedZones,
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Used Bytes',
        datasource='${datasource}',
        gridPosition={ x: 6, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='bytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Bytes currently stored across all buckets (usage metric).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_bytes and on (zone) %s)' % $.rgwS3SelectedZones,
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Object Count',
        datasource='${datasource}',
        gridPosition={ x: 9, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='short',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Number of objects currently stored across all buckets (usage metric).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_objects and on (zone) %s)' % $.rgwS3SelectedZones,
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Avg Object Size',
        datasource='${datasource}',
        gridPosition={ x: 12, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='bytes',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Average size per stored object (used bytes / object count, usage metrics).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_bytes and on (zone) %(z)s) / (sum(ceph_rgw_bucket_usage_objects and on (zone) %(z)s) > 0)' % { z: $.rgwS3SelectedZones },
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='PUT / GET Bytes',
        datasource='${datasource}',
        gridPosition={ x: 15, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='bytes',
        orientation='vertical',
        justifyMode='auto',
        textMode='value_and_name',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Cumulative bytes uploaded (PUT) and downloaded (GET) across all buckets and RGW daemons (per-bucket performance counters).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='PUT',
          range=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_get_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='GET',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='PUT / GET Ops',
        datasource='${datasource}',
        gridPosition={ x: 18, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='short',
        orientation='vertical',
        justifyMode='auto',
        textMode='value_and_name',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        description='Cumulative PUT and GET object operation counts across all buckets and RGW daemons (per-bucket performance counters).',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_put_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='PUT',
          range=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_get_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='GET',
          range=true
        ),
      ]),

      $.addStatPanel(
        title='Compression Ratio',
        datasource='${datasource}',
        gridPosition={ x: 21, y: 1, w: 3, h: 4 },
        graphMode='area',
        colorMode='none',
        unit='suffix::1',
        orientation='auto',
        justifyMode='auto',
        thresholdsMode='absolute',
        pluginVersion='9.4.7',
        decimals=null,
        description='Used Bytes : Utilized Bytes (size after compression and encryption), shown as N:1. Values above 1:1 mean data is compressed.',
      ).addThresholds([
        { color: 'green', value: null },
      ])
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_bucket_usage_bytes and on (zone) %(z)s) / (sum(ceph_rgw_bucket_usage_bytes_utilized and on (zone) %(z)s) > 0)' % { z: $.rgwS3SelectedZones },
          datasource='${datasource}',
          legendFormat='__auto',
          range=true
        ),
      ]),

      // ── Row: Request Activity ──────────────────────────────────────────────
      $.addRowSchema(false, true, 'Request Activity') + { gridPos: { x: 0, y: 5, w: 24, h: 1 } },

      $.addBarGaugePanel(
        title='Total Operations by Type',
        datasource='${datasource}',
        gridPosition={ x: 0, y: 6, w: 8, h: 8 },
        unit='short',
        thresholds=[{ color: 'green', value: null }]
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_buckets_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Buckets',
          range=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Put Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_get_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Get Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_bucket_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Buckets',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_copy_obj_ops *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Copy Objects',
          range=true
        ),
      ]) + { fieldConfig+: { defaults+: { color: { mode: 'thresholds' } } } }
      + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },

      $.addBarGaugePanel(
        title='Total Bandwidth by Operation',
        datasource='${datasource}',
        gridPosition={ x: 8, y: 6, w: 8, h: 8 },
        unit='bytes',
        thresholds=[{ color: 'green', value: null }]
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Put Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_per_bucket_get_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Get Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Objects',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_copy_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Copy Objects',
          range=true
        ),
      ]) + { fieldConfig+: { defaults+: { color: { mode: 'thresholds' } } } }
      + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },

      $.addBarGaugePanel(
        title='Total Latency by Operation',
        datasource='${datasource}',
        gridPosition={ x: 16, y: 6, w: 8, h: 8 },
        unit='ms',
        thresholds=[{ color: 'green', value: null }]
      )
      .addTargets([
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_list_buckets_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='List Bucket',
          range=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_put_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Put Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_get_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Get Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Object',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_del_bucket_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Delete Bucket',
          range=false,
          instant=true
        ),
        $.addTargetSchema(
          expr='sum(ceph_rgw_op_copy_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource='${datasource}',
          legendFormat='Copy Object',
          range=true
        ),
      ]) + { fieldConfig+: { defaults+: { color: { mode: 'thresholds' } } } }
      + { options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' } },

      // ── Row: Users Usage and Performance ───────────────────────────────────
      $.addRowSchema(false, true, 'Users Usage and Performance') + { gridPos: { x: 0, y: 14, w: 24, h: 1 } },


      // Top 5 tables from usage metrics, filtered by the zones of the selected $rgw_instance.
      $.rgwS3TopTable(
        title='Top Users by Usage',
        description='Top 5 users by total bytes stored in all buckets they own. Source: usage metrics (ceph_rgw_bucket_usage_bytes), filtered by the zones of the selected RGW servers. Click a user to open S3 Analytics – Users & Buckets filtered to that user.',
        gridPosition={ h: 8, w: 6, x: 0, y: 15 },
        entity='user',
        metric='ceph_rgw_bucket_usage_bytes',
        valueName='Usage',
        unit='bytes'
      ),

      $.rgwS3TopTable(
        title='Top Users by Object Count',
        description='Top 5 users by number of objects stored in all buckets they own. Source: usage metrics (ceph_rgw_bucket_usage_objects), filtered by the zones of the selected RGW servers. Click a user to open S3 Analytics – Users & Buckets filtered to that user.',
        gridPosition={ h: 8, w: 6, x: 6, y: 15 },
        entity='user',
        metric='ceph_rgw_bucket_usage_objects',
        valueName='Object Count',
        unit='short'
      ),

      $.addTableExtended(
        datasource='${datasource}',
        title='Bandwidth per User',
        gridPosition={ h: 8, w: 6, x: 12, y: 15 },
        description='PUT bytes per user across the selected RGW daemons. Click a user name to open S3 Analytics – Users & Buckets filtered to that user.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 0,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'Bandwidth' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          $.rgwS3HideInstance,
          {
            matcher: { id: 'byName', options: 'user' },
            properties: [
              { id: 'displayName', value: 'User' },
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'User' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Bandwidth' },
              { id: 'unit', value: 'bytes' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #A' },
            properties: [
              { id: 'displayName', value: 'Bandwidth' },
              { id: 'unit', value: 'bytes' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bandwidth' },
            properties: [{ id: 'unit', value: 'bytes' }],
          },
        ],
        pluginVersion='9.4.7'
      ).addTransformations([
        { id: 'organize', options: { excludeByName: { Time: true, time: true, __name__: true } } },
      ]).addTargets([
        $.addTargetSchema(
          expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_put_obj_bytes *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
      ]),

      $.addTableExtended(
        datasource='${datasource}',
        title='Latency(ms) Per User',
        gridPosition={ h: 8, w: 6, x: 18, y: 15 },
        description='PUT operation latency per user across the selected RGW daemons. Click a user name to open S3 Analytics – Users & Buckets filtered to that user.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 0,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'Latency (ms)' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          $.rgwS3HideInstance,
          {
            matcher: { id: 'byName', options: 'user' },
            properties: [
              { id: 'displayName', value: 'User' },
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'User' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Latency (ms)' },
              { id: 'unit', value: 'ms' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #A' },
            properties: [
              { id: 'displayName', value: 'Latency (ms)' },
              { id: 'unit', value: 'ms' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Latency (ms)' },
            properties: [{ id: 'unit', value: 'ms' }],
          },
        ],
        pluginVersion='9.4.7'
      ).addTransformations([
        { id: 'organize', options: { excludeByName: { Time: true, time: true, __name__: true } } },
      ]).addTargets([
        $.addTargetSchema(
          expr='sum by (user, ceph_daemon) (ceph_rgw_op_per_user_put_obj_lat_sum *\n    on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s})' % $.matchers(),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
      ]),

      // ── Row: Buckets Usage and Performance ─────────────────────────────────
      $.addRowSchema(false, true, 'Buckets Usage and Performance') + { gridPos: { x: 0, y: 23, w: 24, h: 1 } },


      $.rgwS3TopTable(
        title='Top Buckets by Usage',
        description='Top 5 buckets by bytes stored. Source: usage metrics (ceph_rgw_bucket_usage_bytes), filtered by the zones of the selected RGW servers. Click a bucket to open S3 Analytics – Users & Buckets filtered to that bucket.',
        gridPosition={ h: 8, w: 8, x: 0, y: 24 },
        entity='bucket',
        metric='ceph_rgw_bucket_usage_bytes',
        valueName='Usage',
        unit='bytes'
      ),

      $.rgwS3TopTable(
        title='Top Buckets by Object Count',
        description='Top 5 buckets by number of objects stored. Source: usage metrics (ceph_rgw_bucket_usage_objects), filtered by the zones of the selected RGW servers. Click a bucket to open S3 Analytics – Users & Buckets filtered to that bucket.',
        gridPosition={ h: 8, w: 8, x: 8, y: 24 },
        entity='bucket',
        metric='ceph_rgw_bucket_usage_objects',
        valueName='Object Count',
        unit='short'
      ),

      $.addTableExtended(
        datasource='${datasource}',
        title='Bandwidth per Bucket',
        gridPosition={ h: 8, w: 8, x: 0, y: 32 },
        description='PUT bytes per bucket across the selected RGW daemons. Click a bucket name to open S3 Analytics – Users & Buckets filtered to that bucket.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 0,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'Bandwidth' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          $.rgwS3HideInstance,
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [
              { id: 'displayName', value: 'Bucket' },
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-bucket=${__value.raw}&var-user=${__data.fields.owner}&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-bucket=${__value.raw}&var-user=${__data.fields.owner}&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'owner' },
            properties: [
              { id: 'displayName', value: 'User' },
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Bandwidth' },
              { id: 'unit', value: 'bytes' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #A' },
            properties: [
              { id: 'displayName', value: 'Bandwidth' },
              { id: 'unit', value: 'bytes' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bandwidth' },
            properties: [{ id: 'unit', value: 'bytes' }],
          },
        ],
        pluginVersion='9.4.7'
      ).addTransformations([
        { id: 'organize', options: { excludeByName: { Time: true, time: true, __name__: true }, indexByName: { bucket: 0, Bucket: 0, owner: 1, User: 1 }, renameByName: {} } },
      ]).addTargets([
        $.addTargetSchema(
          expr=$.rgwS3BucketPerfByOwner('ceph_rgw_op_per_bucket_put_obj_bytes'),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
      ]),

      $.addTableExtended(
        datasource='${datasource}',
        title='Latency(ms) Per Bucket',
        gridPosition={ h: 8, w: 8, x: 16, y: 24 },
        description='PUT operation latency per bucket across the selected RGW daemons. Click a bucket name to open S3 Analytics – Users & Buckets filtered to that bucket.',
        options={
          footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
          frameIndex: 0,
          showHeader: true,
          sortBy: [{ desc: true, displayName: 'Latency (ms)' }],
        },
        custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
        thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
        overrides=[
          $.rgwS3HideInstance,
          {
            matcher: { id: 'byName', options: 'bucket' },
            properties: [
              { id: 'displayName', value: 'Bucket' },
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-bucket=${__value.raw}&var-user=${__data.fields.owner}&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Bucket' },
            properties: [
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-bucket=${__value.raw}&var-user=${__data.fields.owner}&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'owner' },
            properties: [
              { id: 'displayName', value: 'User' },
              {
                id: 'links',
                value: [{
                  title: 'Open ${__value.raw}',
                  url: '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets?var-user=${__value.raw}&var-bucket=All&var-rgw_instance=${__data.fields.ceph_daemon}&${__url_time_range}',
                  targetBlank: false,
                }],
              },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value' },
            properties: [
              { id: 'displayName', value: 'Latency (ms)' },
              { id: 'unit', value: 'ms' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Value #A' },
            properties: [
              { id: 'displayName', value: 'Latency (ms)' },
              { id: 'unit', value: 'ms' },
            ],
          },
          {
            matcher: { id: 'byName', options: 'Latency (ms)' },
            properties: [{ id: 'unit', value: 'ms' }],
          },
        ],
        pluginVersion='9.4.7'
      ).addTransformations([
        { id: 'organize', options: { excludeByName: { Time: true, time: true, __name__: true }, indexByName: { bucket: 0, Bucket: 0, owner: 1, User: 1 }, renameByName: {} } },
      ]).addTargets([
        $.addTargetSchema(
          expr=$.rgwS3BucketPerfByOwner('ceph_rgw_op_per_bucket_put_obj_lat_sum'),
          datasource={ type: 'prometheus', uid: '${datasource}' },
          format='table',
          hide=false,
          exemplar=false,
          instant=true,
          interval='',
          legendFormat='',
          range=false,
        ),
      ]),
    ]),
}
