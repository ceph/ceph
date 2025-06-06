{
  /**
   * Creates a [Time series panel](https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/time-series/).
   *
   * @name timeseries_panel.new
   *
   * @param title (default `''`) Panel title.
   * @param description (default null) Panel description.
   */
  new(
    title='',
    description=null,
    pluginVersion='9.1.3',
    gridPos={},
    datasource='',
    colorMode='palette-classic',
    axisCenteredZero=false,
    axisColorMode='text',
    axisLabel='',
    axisPlacement='auto',
    barAlignment=0,
    drawStyle='line',
    fillOpacity=8,
    gradientMode='none',
    lineInterpolation='linear',
    lineWidth=1,
    pointSize=0,
    scaleDistributionType='linear',
    showPoints='',
    spanNulls=false,
    stackingGroup='A',
    stackingMode='none',
    thresholdsStyleMode='off',
    decimals=null,
    thresholdsMode='absolute',
    unit='none',
    tooltip={},
    legend={},
    displayMode='list',
    placement='bottom',
    showLegend=true,
    min=null,
    scaleDistributionLog=null,
    sortBy=null,
    sortDesc=null,
    noValue=null,
  ):: {
    title: title,
    type: 'timeseries',
    [if description != null then 'description']: description,
    pluginVersion: pluginVersion,
    gridPos: gridPos,
    datasource: datasource,
    fieldConfig: {
      defaults: {
        color: { mode: colorMode },
        custom: {
          axisCenteredZero: axisCenteredZero,
          axisColorMode: axisColorMode,
          axisLabel: axisLabel,
          axisPlacement: axisPlacement,
          barAlignment: barAlignment,
          drawStyle: drawStyle,
          fillOpacity: fillOpacity,
          gradientMode: gradientMode,
          hideFrom: {
            legend: false,
            tooltip: false,
            viz: false,
          },
          lineInterpolation: lineInterpolation,
          lineWidth: lineWidth,
          pointSize: pointSize,
          scaleDistribution: {
            [if scaleDistributionLog != null then 'scaleDistributionLog']: scaleDistributionLog,
            type: scaleDistributionType,
          },
          showPoints: showPoints,
          spanNulls: spanNulls,
          stacking: {
            group: stackingGroup,
            mode: stackingMode,
          },
          thresholdsStyle: {
            mode: thresholdsStyleMode,
          },
        },
        mappings: [],
        [if decimals != null then 'decimals']: decimals,
        [if min != null then 'min']: min,
        thresholds: {
          mode: thresholdsMode,
          steps: [],
        },
        [if noValue != null then 'noValue']: noValue,
        unit: unit,
      },
      overrides: [],
    },
    seriesOverrides: [],
    options: {
      legend: {
        calcs: [],
        displayMode: displayMode,
        placement: placement,
        showLegend: showLegend,
        [if sortBy != null then 'sortBy']: sortBy,
        [if sortDesc != null then 'sortDesc']: sortDesc,
      },
      tooltip: tooltip,
    },
    // Overrides
    addOverride(
      matcher=null,
      properties=null,
    ):: self {
      fieldConfig+: {
        overrides+: [
          {
            [if matcher != null then 'matcher']: matcher,
            [if properties != null then 'properties']: properties,
          },
        ],
      },
    },
    // thresholds
    addThreshold(step):: self {
      fieldConfig+: { defaults+: { thresholds+: { steps+: [step] } } },
    },
    // mappings
    _nextMapping:: 0,
    addMapping(mapping):: self {
      local nextMapping = super._nextMapping,
      _nextMapping: nextMapping + 1,
      fieldConfig+: { defaults+: { mappings+: [mapping { id: nextMapping }] } },
    },
    addCalc(calc):: self {
      options+: { legend+: { calcs+: [calc] } },
    },
    _nextTarget:: 0,
    addTarget(target):: self {
      // automatically ref id in added targets.
      local nextTarget = super._nextTarget,
      _nextTarget: nextTarget + 1,
      targets+: [target { refId: std.char(std.codepoint('A') + nextTarget) }],
    },
    addTargets(targets):: std.foldl(function(p, t) p.addTarget(t), targets, self),
    addThresholds(steps):: std.foldl(function(p, s) p.addThreshold(s), steps, self),
    addMappings(mappings):: std.foldl(function(p, m) p.addMapping(m), mappings, self),
    addCalcs(calcs):: std.foldl(function(p, t) p.addCalc(t), calcs, self),
    addOverrides(overrides):: std.foldl(function(p, o) p.addOverride(o.matcher, o.properties), overrides, self),
    addSeriesOverride(override):: self {
      seriesOverrides+: [override],
    },
  },
}
