local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  // ── Shared building blocks ───────────────────────────────────────────────
  local promDs = { type: 'prometheus', uid: '${datasource}' },
  local dashUrl = '/d/s3usrbktAnlt/ceph-object-s3-analytics-users-buckets',
  local keepContext = '${rgw_instance:queryparam}&${__url_time_range}',
  local userLink = [{ title: 'Open ${__value.raw}', url: dashUrl + '?var-user=${__value.raw}&' + keepContext, targetBlank: false }],
  local bucketLink = [{ title: 'Open ${__value.raw}', url: dashUrl + '?var-bucket=${__value.raw}&' + keepContext, targetBlank: false }],
  local bucketOfUserLink = [{ title: 'Open ${__value.raw}', url: dashUrl + '?var-bucket=${__value.raw}&var-user=${__data.fields.User}&' + keepContext, targetBlank: false }],

  // Usage metrics have no daemon label; $rgw_instance is applied through the zone of the selected
  // daemons. $user maps to the 'owner' label and $bucket to the 'bucket' label.
  local usageSel = '{owner=~"$user", bucket=~"$bucket"}',
  local usage(metric) = '(%s%s and on (zone) %s)' % [metric, usageSel, $.rgwS3SelectedZones],
  // Bucket quota series share the usage labels. Owner quota series have no bucket label: they are
  // restricted to the selected user(s) and to the owners of the selected bucket(s).
  local userQuota(metric) = '(%s{owner=~"$user"} and on (zone) %s and on (zone, tenant, owner) ceph_rgw_bucket_usage_bytes{bucket=~"$bucket"})' % [metric, $.rgwS3SelectedZones],
  // Usage of the selected user(s) over all of their buckets, to compare with a per-user quota.
  local userUsage(metric) = '(%s{owner=~"$user"} and on (zone) %s)' % [metric, $.rgwS3SelectedZones],
  local meta = 'ceph_rgw_metadata{ceph_daemon=~"$rgw_instance", %(matchers)s}' % $.matchers(),

  // Performance counters carry no zone label. Their zone is taken from the serving RGW daemon
  // (instance_id -> ceph_rgw_metadata.zone_id), which also applies the $rgw_instance filter.
  local withZone(inner) = '((%s) * on (instance_id) group_left (ceph_daemon, zone) %s)' % [inner, $.rgwS3SelectedZones],
  // Per-bucket perf counters are matched to the usage series of the same bucket name, tenant and
  // zone, so same-named buckets of different users/zones/tenants stay separate and the owner
  // becomes a label. $bucket filters the counters directly; $user filters through the owner.
  local bucketPerf(inner) = '(%s * on (bucket, tenant, zone) group_left (owner) group by (bucket, tenant, zone, owner) (ceph_rgw_bucket_usage_bytes%s))' % [withZone(inner), usageSel],
  // Per-user perf counters are matched to the users owning (selected) buckets in the same zone.
  // The owner label of tenanted users is "tenant$uid", the perf counter user label is the uid and the
  // tenant is a separate label, so the user filter is applied only through this join.
  // Values are always the user's totals over all of the user's buckets.
  local userPerf(inner) = '(%s and on (user, tenant, zone) label_replace(ceph_rgw_bucket_usage_bytes%s, "user", "$1", "owner", "(?:[^$]*\\\\$)?(.+)"))' % [withZone(inner), usageSel],

  local pctThresholds = [{ color: 'green', value: null }, { color: 'orange', value: 0.8 }, { color: 'red', value: 0.95 }],
  local stat(title, desc, x, w, unit, expr, decimals=0, y=1, h=3, noValue=null, thresholds=[{ color: 'green', value: null }], colorMode='none', graphMode='none', orientation='auto', textMode='auto') =
    // expr is a single query, or a list of { expr, legend } for several series in one panel
    local tgts = if std.isString(expr) then [{ expr: expr, legend: '__auto' }] else expr;
    $.addStatPanel(
      title=title,
      datasource='${datasource}',
      gridPosition={ x: x, y: y, w: w, h: h },
      graphMode=graphMode,
      colorMode=colorMode,
      unit=unit,
      orientation=orientation,
      justifyMode='auto',
      textMode=textMode,
      thresholdsMode='absolute',
      pluginVersion='9.4.7',
      description=desc,
      decimals=decimals,
      noValue=noValue,
    ).addThresholds(thresholds)
    .addTargets([
      $.addTargetSchema(expr=t.expr, datasource='${datasource}', legendFormat=t.legend, range=true)
      for t in tgts
    ]),

  // Overview-style stat: same size, sparkline and layout as the Overview dashboard.
  local ov(title, desc, x, unit, expr, decimals=0, orientation='auto', textMode='auto') =
    stat(title, desc, x, 3, unit, expr, decimals=decimals, h=4, graphMode='area', orientation=orientation, textMode=textMode),

  local ts(title, desc, pos, unit, targets, decimals=2, tableLegend=false) =
    $.timeSeriesPanel(
      lineInterpolation='linear',
      lineWidth=1,
      drawStyle='line',
      axisPlacement='auto',
      title=title,
      datasource='${datasource}',
      gridPosition=pos,
      fillOpacity=0,
      pointSize=5,
      showPoints='auto',
      unit=unit,
      displayMode='list',
      showLegend=true,
      placement='bottom',
      tooltip={ mode: 'multi', sort: 'desc' },
      stackingMode='none',
      spanNulls=true,
      decimals=decimals,
      thresholdsMode='absolute',
      description=desc,
    )
    .addThresholds([{ color: 'green' }])
    .addTargets([
      $.addTargetSchema(
        expr=t.expr,
        datasource='${datasource}',
        format='time_series',
        instant=false,
        legendFormat=t.legend,
        range=true,
      )
      for t in targets
    ]) + {
      options: {
        legend: if tableLegend then { displayMode: 'table', placement: 'bottom', showLegend: true, calcs: ['lastNotNull'] } else { displayMode: 'list', placement: 'bottom', showLegend: true },
        tooltip: { mode: 'multi', sort: 'desc' },
      },
    },

  local bar(title, desc, pos, unit, expr, legend) =
    $.addBarGaugePanel(
      title=title,
      description=desc,
      datasource='${datasource}',
      gridPosition=pos,
      unit=unit,
      thresholds={ color: 'green', value: null }
    )
    .addTargets([
      $.addTargetSchema(expr=expr, datasource='${datasource}', legendFormat=legend, range=false, instant=true),
    ]) + {
      fieldConfig+: { defaults+: { color: { mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green', value: null }] } } },
      options: { orientation: 'horizontal', reduceOptions: { calcs: ['lastNotNull'] }, displayMode: 'gradient' },
    },

  // One row per combination of the key labels, one column per metric.
  // The instant queries are merged and then grouped by the identifying labels (max over the
  // single populated value of each column), so each key combination is a single row instead of
  // one row per metric.
  //   keys: [{ label, name, link }]   cols: [{ name, unit, expr }]
  local table(title, desc, pos, keys, cols) =
    local nk = std.length(keys);
    local nc = std.length(cols);
    local refs = [std.char(65 + i) for i in std.range(0, nc - 1)];
    local valueField(i) = 'Value #' + refs[i] + ' (max)';
    $.addTableExtended(
      datasource='${datasource}',
      title=title,
      gridPosition=pos,
      description=desc,
      options={
        footer: { fields: '', reducer: ['sum'], countRows: false, enablePagination: false, show: false },
        frameIndex: 0,
        showHeader: true,
        sortBy: [{ desc: true, displayName: cols[0].name }],
      },
      custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
      thresholds={ mode: 'absolute', steps: [{ color: 'green', value: null }] },
      overrides=[
        { matcher: { id: 'byName', options: c.name }, properties: [{ id: 'unit', value: c.unit }] }
        for c in cols
      ] + [
        { matcher: { id: 'byName', options: k.name }, properties: [{ id: 'links', value: k.link }] }
        for k in keys
        if std.objectHas(k, 'link')
      ],
      pluginVersion='9.4.7'
    ).addTransformations([
      { id: 'merge', options: {} },
      {
        id: 'groupBy',
        options: {
          fields: {
            [k.label]: { aggregations: [], operation: 'groupby' }
            for k in keys
          } + {
            ['Value #' + refs[i]]: { aggregations: ['max'], operation: 'aggregate' }
            for i in std.range(0, nc - 1)
          },
        },
      },
      {
        id: 'organize',
        options: {
          excludeByName: {},
          indexByName: {
            [keys[i].label]: i
            for i in std.range(0, nk - 1)
          } + {
            [valueField(i)]: nk + i
            for i in std.range(0, nc - 1)
          },
          renameByName: {
            [k.label]: k.name
            for k in keys
          } + {
            [valueField(i)]: cols[i].name
            for i in std.range(0, nc - 1)
          },
        },
      },
      { id: 'filterFieldsByName', options: { include: { names: [k.name for k in keys] + [c.name for c in cols] } } },
      { id: 'sortBy', options: { fields: [{ desc: true, displayName: cols[0].name }] } },
    ]).addTargets([
      $.addTargetSchema(
        expr=c.expr,
        datasource=promDs,
        format='table',
        hide=false,
        exemplar=false,
        instant=true,
        interval='',
        legendFormat='',
        range=false,
      )
      for c in cols
    ]),

  local opTypes = [
    { key: 'put', name: 'PUT' },
    { key: 'get', name: 'GET' },
    { key: 'list', name: 'List' },
    { key: 'del', name: 'DELETE' },
    { key: 'copy', name: 'COPY' },
  ],

  // All operation panels for one entity ('user' or 'bucket'), in this order: summary tables, top
  // values, then graphs.
  //   ent: { label, name, prefix, scope(inner), link }, y: first panel row, note: scope limitation
  local entityPanels(ent, y, note) =
    local sel = ent.sel;
    local rate(m) = ent.scope('rate(%s_%s%s[$__rate_interval])' % [ent.prefix, m, sel]);
    local cum(m) = ent.scope('%s_%s%s' % [ent.prefix, m, sel]);
    local by(e) = 'sum by (%s) (%s)' % [ent.groupBy, e];
    local legend = ent.legend;
    local lname = std.asciiLower(ent.name);
    local src = ' (per-' + ent.label + ' performance counters). ';
    [
      // Summary
      table(
        ent.name + ' Operation Summary',
        'Cumulative operation counts per ' + lname + ' (per-' + ent.label + ' performance counters, summed over the selected RGW instance). ' + note,
        { x: 0, y: y, w: 12, h: 8 },
        ent.keys,
        [{ name: o.name, unit: 'none', expr: by(cum(o.key + '_obj_ops')) } for o in opTypes]
      ),
      table(
        ent.name + ' Average Latency (ms)',
        'Average latency per operation (lat_sum / lat_count of the per-' + ent.label + ' performance counters, converted to milliseconds) per ' + lname + '. ' + note,
        { x: 12, y: y, w: 12, h: 8 },
        ent.keys,
        [{ name: o.name + ' (ms)', unit: 'ms', expr: '1000 * %s / (%s > 0)' % [by(cum(o.key + '_obj_lat_sum')), by(cum(o.key + '_obj_lat_count'))] } for o in opTypes]
      ),
      // Top values
      bar('Top 5 ' + ent.name + ' PUTs by Operations', 'Cumulative PUT operations per ' + lname + src + note, { x: 0, y: y + 8, w: 12, h: 8 }, 'none', 'topk(5, %s)' % by(cum('put_obj_ops')), legend),
      bar('Top 5 ' + ent.name + ' GETs by Operations', 'Cumulative GET operations per ' + lname + src + note, { x: 12, y: y + 8, w: 12, h: 8 }, 'none', 'topk(5, %s)' % by(cum('get_obj_ops')), legend),
      // Graphs
      ts(ent.name + ' PUT Throughput', 'Upload throughput (bytes/s) per ' + lname + ' from per-' + ent.label + ' performance counters. This is traffic, not stored capacity. ' + note, { x: 0, y: y + 16, w: 6, h: 7 }, 'Bps', [{ expr: by(rate('put_obj_bytes')), legend: legend }]),
      ts(ent.name + ' GET Throughput', 'Download throughput (bytes/s) per ' + lname + ' from per-' + ent.label + ' performance counters. This is traffic, not stored capacity. ' + note, { x: 6, y: y + 16, w: 6, h: 7 }, 'Bps', [{ expr: by(rate('get_obj_bytes')), legend: legend }]),
      ts(ent.name + ' PUT Request Rate', 'PUT operations per second per ' + lname + ' from per-' + ent.label + ' performance counters. ' + note, { x: 12, y: y + 16, w: 6, h: 7 }, 'ops', [{ expr: by(rate('put_obj_ops')), legend: legend }]),
      ts(ent.name + ' GET Request Rate', 'GET operations per second per ' + lname + ' from per-' + ent.label + ' performance counters. ' + note, { x: 18, y: y + 16, w: 6, h: 7 }, 'ops', [{ expr: by(rate('get_obj_ops')), legend: legend }]),
      ts(ent.name + ' Copy Throughput', 'Copy throughput (bytes/s) per ' + lname + src + 'Traffic, not stored capacity. ' + note, { x: 0, y: y + 23, w: 12, h: 7 }, 'Bps', [{ expr: by(rate('copy_obj_bytes')), legend: legend }]),
      ts(ent.name + ' Delete Throughput', 'Delete throughput (bytes/s) per ' + lname + src + 'Traffic, not stored capacity. ' + note, { x: 12, y: y + 23, w: 12, h: 7 }, 'Bps', [{ expr: by(rate('del_obj_bytes')), legend: legend }]),
      ts(ent.name + ' List Request Rate', 'List operations per second per ' + lname + src + note, { x: 0, y: y + 30, w: 8, h: 7 }, 'ops', [{ expr: by(rate('list_obj_ops')), legend: legend }]),
      ts(ent.name + ' Delete Request Rate', 'Delete operations per second per ' + lname + src + note, { x: 8, y: y + 30, w: 8, h: 7 }, 'ops', [{ expr: by(rate('del_obj_ops')), legend: legend }]),
      ts(ent.name + ' Copy Request Rate', 'Copy operations per second per ' + lname + src + note, { x: 16, y: y + 30, w: 8, h: 7 }, 'ops', [{ expr: by(rate('copy_obj_ops')), legend: legend }]),
    ],

  local userNote = 'Respects RGW instance and user; when a bucket is selected only the owners of that bucket are shown, with their totals over all of their buckets.',
  local bucketNote = "Respects RGW instance and bucket; when a user is selected only that user's buckets are shown. Counters are matched to buckets by name, tenant and the zone of the serving RGW daemon, so same-named buckets are listed separately per owner.",

  local userEnt = { label: 'user', groupBy: 'user', legend: '{{user}}', name: 'User', prefix: 'ceph_rgw_op_per_user', scope: userPerf, sel: '', keys: [{ label: 'user', name: 'User', link: userLink }] },
  local bucketEnt = { label: 'bucket', groupBy: 'bucket, owner', legend: '{{bucket}}', name: 'Bucket', prefix: 'ceph_rgw_op_per_bucket', scope: bucketPerf, sel: '{bucket=~"$bucket"}', keys: [{ label: 'bucket', name: 'Bucket', link: bucketOfUserLink }, { label: 'owner', name: 'User', link: userLink }] },

  'rgw-s3-analytics-users-buckets.json':
    $.dashboardSchema(
      'Ceph Object - S3 Analytics - Users & Buckets',
      'Combined per-user and per-bucket S3 usage and performance. Filters: RGW instance, user and bucket. Usage and quota panels (stored/used bytes, objects, compression, bucket and user quotas) come from the usage and quota metrics and respect all three filters. User-level performance panels respect RGW instance and user, and are narrowed to the owners of the selected bucket(s). Bucket-level performance panels respect RGW instance and bucket, and are narrowed to the buckets of the selected user(s).',
      's3usrbktAnlt',
      'now-1h',
      '1m',
      22,
      $._config.dashboardTags,
      ''
    )
    .addAnnotation(
      $.addAnnotationSchema(
        1, '-- Grafana --', true, true, 'rgba(0, 211, 255, 1)', 'Annotations & Alerts', 'dashboard'
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
    .addTemplate($.addClusterTemplate())
    .addTemplate(
      // Single select without "All": Grafana selects the first (sorted) value on load unless the
      // Overview dashboard passes var-rgw_instance in the link.
      $.addTemplateSchema('rgw_instance',
                          '$datasource',
                          'label_values(ceph_rgw_metadata{%(matchers)s}, ceph_daemon)' % $.matchers(),
                          2,
                          false,
                          1,
                          null,
                          '')
    )
    .addTemplate(
      // user = 'owner' label of the usage metrics. Usage metrics only carry the zone, so the series are
      // limited to the zone of the selected $rgw_instance with a join. The query depends directly on
      // $rgw_instance (no intermediate variable), so the list is correct on the first load too.
      $.addTemplateSchema('user',
                          '$datasource',
                          'query_result(group by (owner) (ceph_rgw_bucket_usage_bytes and on (zone) %s))' % $.rgwS3SelectedZones,
                          2,
                          true,
                          0,
                          'User',
                          '/owner="([^"]*)"/',
                          allValues='.*',
                          current='all')
    )
    .addTemplate(
      // buckets cascade from the selected user(s) and are limited to the zone of the selected $rgw_instance
      $.addTemplateSchema('bucket',
                          '$datasource',
                          'query_result(group by (bucket) (ceph_rgw_bucket_usage_bytes{owner=~"$user"} and on (zone) %s))' % $.rgwS3SelectedZones,
                          2,
                          true,
                          0,
                          'Bucket',
                          '/bucket="([^"]*)"/',
                          allValues='.*',
                          current='all')
    )

    .addPanels([
      // ── Overview (same panels and format as the S3 Analytics - Overview dashboard) ──
      $.addRowSchema(false, true, 'Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },

      ov('Buckets', 'Number of buckets in the selection (usage metric). Respects RGW instance, user and bucket.', 0, 'none', 'count(%s)' % usage('ceph_rgw_bucket_usage_bytes')),
      ov('Users', 'Number of users owning at least one bucket in the selection (usage metric). Respects RGW instance, user and bucket.', 3, 'none', 'count(count by (owner) (%s))' % usage('ceph_rgw_bucket_usage_bytes')),
      ov('Used Bytes', 'Bytes currently stored by the selected users/buckets (usage metric). Respects RGW instance, user and bucket.', 6, 'decbytes', 'sum(%s)' % usage('ceph_rgw_bucket_usage_bytes')),
      ov('Object Count', 'Number of objects currently stored by the selected users/buckets (usage metric). Respects RGW instance, user and bucket.', 9, 'none', 'sum(%s)' % usage('ceph_rgw_bucket_usage_objects')),
      ov('Avg Object Size', 'Average size per stored object (used bytes / object count, usage metrics). Respects RGW instance, user and bucket.', 12, 'decbytes', 'sum(%s) / (sum(%s) > 0)' % [usage('ceph_rgw_bucket_usage_bytes'), usage('ceph_rgw_bucket_usage_objects')]),
      ov(
        'PUT / GET Bytes',
        'Cumulative bytes uploaded (PUT) and downloaded (GET) in the selected buckets (per-bucket performance counters). ' + bucketNote,
        15,
        'decbytes',
        [
          { expr: 'sum(%s)' % bucketPerf('ceph_rgw_op_per_bucket_put_obj_bytes{bucket=~"$bucket"}'), legend: 'PUT' },
          { expr: 'sum(%s)' % bucketPerf('ceph_rgw_op_per_bucket_get_obj_bytes{bucket=~"$bucket"}'), legend: 'GET' },
        ],
        orientation='vertical',
        textMode='value_and_name'
      ),
      ov(
        'PUT / GET Ops',
        'Cumulative PUT and GET object operation counts in the selected buckets (per-bucket performance counters). ' + bucketNote,
        18,
        'none',
        [
          { expr: 'sum(%s)' % bucketPerf('ceph_rgw_op_per_bucket_put_obj_ops{bucket=~"$bucket"}'), legend: 'PUT' },
          { expr: 'sum(%s)' % bucketPerf('ceph_rgw_op_per_bucket_get_obj_ops{bucket=~"$bucket"}'), legend: 'GET' },
        ],
        orientation='vertical',
        textMode='value_and_name'
      ),
      ov('Compression Ratio', 'Used Bytes : Utilized Bytes (size after compression and encryption), shown as N:1. Values above 1:1 mean data is compressed. Respects RGW instance, user and bucket.', 21, 'suffix::1', 'sum(%s) / (sum(%s) > 0)' % [usage('ceph_rgw_bucket_usage_bytes'), usage('ceph_rgw_bucket_usage_bytes_utilized')], decimals=null),

      // ── Quota summary (bucket quotas on the left, user quotas on the right) ──
      $.addRowSchema(false, true, 'Quota Summary') + { gridPos: { x: 0, y: 5, w: 24, h: 1 } },

      stat('Bucket Max Bytes', 'Sum of the byte quotas of the selected buckets that have a quota enabled (ceph_rgw_bucket_quota_bytes). Buckets without a quota are not counted. Respects RGW instance, user and bucket.', 0, 3, 'decbytes', 'sum(%s)' % usage('ceph_rgw_bucket_quota_bytes'), y=6, h=3, noValue='N/A'),
      stat('Bucket Max Objects', 'Sum of the object quotas of the selected buckets that have a quota enabled (ceph_rgw_bucket_quota_objects). Buckets without a quota are not counted. Respects RGW instance, user and bucket.', 3, 3, 'none', 'sum(%s)' % usage('ceph_rgw_bucket_quota_objects'), y=6, h=3, noValue='N/A'),
      stat('Bucket Used (Bytes)', 'Used Bytes / byte quota, over the selected buckets that have a byte quota.', 6, 3, 'percentunit', 'sum(%s and on (zone, tenant, owner, bucket) %s) / sum(%s)' % [usage('ceph_rgw_bucket_usage_bytes'), usage('ceph_rgw_bucket_quota_bytes'), usage('ceph_rgw_bucket_quota_bytes')], 1, y=6, h=3, noValue='N/A', thresholds=pctThresholds, colorMode='value'),
      stat('Bucket Used (Objects)', 'Object Count / object quota, over the selected buckets that have an object quota.', 9, 3, 'percentunit', 'sum(%s and on (zone, tenant, owner, bucket) %s) / sum(%s)' % [usage('ceph_rgw_bucket_usage_objects'), usage('ceph_rgw_bucket_quota_objects'), usage('ceph_rgw_bucket_quota_objects')], 1, y=6, h=3, noValue='N/A', thresholds=pctThresholds, colorMode='value'),

      stat('User Max Bytes', 'Sum of the user/account byte quotas (ceph_rgw_owner_quota_bytes) of the selected users, or of the owners of the selected buckets. Users without a quota are not counted. Owner quotas have no bucket label, so the bucket filter only narrows the list of owners.', 12, 3, 'decbytes', 'sum(%s)' % userQuota('ceph_rgw_owner_quota_bytes'), y=6, h=3, noValue='N/A'),
      stat('User Max Objects', 'Sum of the user/account object quotas (ceph_rgw_owner_quota_objects) of the selected users, or of the owners of the selected buckets. Users without a quota are not counted. Owner quotas have no bucket label, so the bucket filter only narrows the list of owners.', 15, 3, 'none', 'sum(%s)' % userQuota('ceph_rgw_owner_quota_objects'), y=6, h=3, noValue='N/A'),
      stat('User Used (Bytes)', "Bytes stored in all buckets of the user(s) / user byte quota, over the users that have a byte quota. Not narrowed to the selected buckets, because the user quota applies to all of the user's buckets.", 18, 3, 'percentunit', 'sum(sum by (zone, tenant, owner) (%s) and on (zone, tenant, owner) %s) / sum(%s)' % [userUsage('ceph_rgw_bucket_usage_bytes'), userQuota('ceph_rgw_owner_quota_bytes'), userQuota('ceph_rgw_owner_quota_bytes')], 1, y=6, h=3, noValue='N/A', thresholds=pctThresholds, colorMode='value'),
      stat('User Used (Objects)', "Objects in all buckets of the user(s) / user object quota, over the users that have an object quota. Not narrowed to the selected buckets, because the user quota applies to all of the user's buckets.", 21, 3, 'percentunit', 'sum(sum by (zone, tenant, owner) (%s) and on (zone, tenant, owner) %s) / sum(%s)' % [userUsage('ceph_rgw_bucket_usage_objects'), userQuota('ceph_rgw_owner_quota_objects'), userQuota('ceph_rgw_owner_quota_objects')], 1, y=6, h=3, noValue='N/A', thresholds=pctThresholds, colorMode='value'),

      // ── Usage trends ──────────────────────────────────────────────────────
      $.addRowSchema(false, true, 'Usage Trends') + { gridPos: { x: 0, y: 9, w: 24, h: 1 } },

      ts('Storage Usage Trend', 'Used Bytes per bucket over time (usage metric, gauge).', { x: 0, y: 10, w: 8, h: 8 }, 'decbytes', [{ expr: 'sum by (owner, bucket) (%s)' % usage('ceph_rgw_bucket_usage_bytes'), legend: '{{bucket}}' }], 2, true),
      ts('Object Count Trend', 'Objects per bucket over time (usage metric, gauge).', { x: 8, y: 10, w: 8, h: 8 }, 'none', [{ expr: 'sum by (owner, bucket) (%s)' % usage('ceph_rgw_bucket_usage_objects'), legend: '{{bucket}}' }], 0, true),
      ts('Compression Ratio Trend', 'Used Bytes : Utilized Bytes per bucket over time.', { x: 16, y: 10, w: 8, h: 8 }, 'suffix::1', [{ expr: 'sum by (owner, bucket) (%s) / (sum by (owner, bucket) (%s) > 0)' % [usage('ceph_rgw_bucket_usage_bytes'), usage('ceph_rgw_bucket_usage_bytes_utilized')], legend: '{{bucket}}' }], 2, true),

      // ── Operations and performance (collapsible): summary, top values, then graphs ──
      $.addRowSchema(false, true, 'User Operations and Performance', collapsed=true)
      .addPanels(entityPanels(userEnt, 19, userNote))
      + { gridPos: { x: 0, y: 18, w: 24, h: 1 } },

      $.addRowSchema(false, true, 'Bucket Operations and Performance', collapsed=true)
      .addPanels(entityPanels(bucketEnt, 20, bucketNote))
      + { gridPos: { x: 0, y: 19, w: 24, h: 1 } },
    ]),
}
