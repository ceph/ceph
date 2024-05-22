{
  /**
   * Creates a pie chart panel.
   *
   * @name pieChartPanel.new
   *
   * @param title The title of the pie chart panel.
   * @param description (default `''`) Description of the panel
   * @param datasource (optional) Datasource
   * @param pieType (default `'pie'`) Type of pie chart (one of pie or donut)
   *
   * @method addTarget(target) Adds a target object.
   */
  new(
    title,
    description='',
    datasource=null,
    gridPos={},
    displayMode='table',
    placement='bottom',
    showLegend=true,
    displayLabels=[],
    tooltip={},
    pieType='pie',
    values=[],
    colorMode='auto',
    overrides=[],
    reduceOptions={},
  ):: {
    type: 'piechart',
    [if description != null then 'description']: description,
    title: title,
    gridPos: gridPos,
    datasource: datasource,
    options: {
      legend: {
        calcs: [],
        values: values,
        displayMode: displayMode,
        placement: placement,
        showLegend: showLegend,
      },
      pieType: pieType,
      tooltip: tooltip,
      displayLabels: displayLabels,
      reduceOptions: reduceOptions,
    },
    fieldConfig: {
      defaults: {
        color: { mode: colorMode },
        mappings: [],
        custom: {
          hideFrom: {
            legend: false,
            tooltip: false,
            viz: false,
          },
        },
      },
      overrides: overrides,
    },
    targets: [
    ],
    _nextTarget:: 0,
    addTarget(target):: self {
      // automatically ref id in added targets.
      local nextTarget = super._nextTarget,
      _nextTarget: nextTarget + 1,
      targets+: [target { refId: std.char(std.codepoint('A') + nextTarget) }],
    },
    addTargets(targets):: std.foldl(function(p, t) p.addTarget(t), targets, self),
  },
}
