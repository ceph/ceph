local g = import 'grafonnet/grafana.libsonnet';

(import 'utils.libsonnet') {
  'hardware.json':
    $.dashboardSchema(
      'Ceph Hardware - Overview',
      'Comprehensive hardware monitoring with temperatures, fans, storage, and firmware from node-proxy',
      'hardware001',
      'now-1h',
      '10s',
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
        title='Ceph Hardware - Overview',
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
      $.addTemplateSchema(
        'hostname',
        '$datasource',
        'label_values(ceph_hardware_health, hostname)',
        1,
        false,
        1,
        'Host',
        ''
      )
    )
    .addTemplate(
      g.template.new(
        'fan_speeds',
        '$datasource',
        'label_values(ceph_hardware_fan_rpm{hostname=~"$hostname"},fan_name)',
        label='',
        refresh='load',
        includeAll=true,
        multi=true,
        allValues='',
        sort=0,
        regex='/.*TACH_OUT/',
        hide=2
      )
    )
    .addPanels([
      // Row 1: Overview (collapsed)
      $.addRowSchema(collapse=true, showTitle=true, title='Overview') + { gridPos: { x: 0, y: 0, w: 24, h: 1 }, panels: [
        // Health
        $.addStatPanel(
          title='Health',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 4, w: 4, x: 0, y: 1 },
          reducerFunction='last',
          noValue='N/A',
        )
        .addTarget($.addTargetSchema(
          'max(ceph_hardware_health)',
          legendFormat='Health'
        ))
        + {
          description: 'Overall hardware health across all hosts',
          fieldConfig: {
            defaults: {
              mappings: [
                { type: 'value', options: { '0': { color: 'green', index: 0, text: 'OK' } } },
                { type: 'range', options: { from: 1, to: 999, result: { color: 'semi-dark-red', index: 1, text: 'NOT OK' } } },
              ],
              noValue: 'N/A',
              thresholds: {
                mode: 'absolute',
                steps: [{ color: 'green' }, { color: 'red', value: 1 }],
              },
            },
          },
          options+: { colorMode: 'background_solid' },
        },

        // CPU Temp
        $.addGaugePanel(
          title='CPU Temp',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 4, y: 1 },
          min=0,
          max=100,
        )
        .addThresholds([
          { color: 'green' },
          { color: '#EAB839', value: 75 },
          { color: 'red', value: 85 },
        ])
        .addTarget($.addTargetSchema(
          'max(ceph_hardware_temperature_celsius{sensor_name=~".*CPU_TEMP"})',
          legendFormat='CPU'
        ))
        + { description: 'Highest CPU temperature across all hosts' },

        // BMC Versions
        $.pieChartPanel(
          title='BMC Versions',
          datasource='$datasource',
          gridPos={ x: 7, y: 1, w: 3, h: 8 }
        )
        .addTarget($.addTargetSchema(
          'count by(version) (ceph_hardware_firmware_info{component=~".*BMC.*"})',
          legendFormat='{{version}}'
        ))
        + {
          fieldConfig+: {
            defaults+: {
              color+: { mode: 'palette-classic' },
            },
          },
          options+: {
            pieType: 'donut',
            legend+: { showLegend: true, placement: 'bottom' },
            displayLabels: ['name', 'value'],
          },
        },

        // BIOS Versions
        $.pieChartPanel(
          title='BIOS Versions',
          datasource='$datasource',
          gridPos={ x: 10, y: 1, w: 3, h: 8 }
        )
        .addTarget($.addTargetSchema(
          'count by(version) (ceph_hardware_firmware_info{component=~".*BIOS.*"})',
          legendFormat='{{version}}'
        ))
        + {
          fieldConfig+: {
            defaults+: {
              color+: { mode: 'palette-classic' },
            },
          },
          options+: {
            pieType: 'donut',
            legend+: { showLegend: true, placement: 'bottom' },
            displayLabels: ['name', 'value'],
          },
        },

        // Drive Types
        $.pieChartPanel(
          title='Drive Types',
          datasource='$datasource',
          gridPos={ x: 13, y: 1, w: 6, h: 8 }
        )
        .addTarget($.addTargetSchema(
          'count by(model) (ceph_hardware_storage_capacity_bytes)',
          legendFormat='{{model}}'
        ))
        + {
          fieldConfig+: {
            defaults+: {
              color+: { mode: 'palette-classic' },
            },
          },
          options+: {
            pieType: 'donut',
            legend+: { showLegend: true, placement: 'right' },
            displayLabels: ['value'],
          },
        },

        // Drive Firmware
        $.pieChartPanel(
          title='Drive Firmware',
          datasource='$datasource',
          gridPos={ x: 19, y: 1, w: 5, h: 8 }
        )
        .addTarget($.addTargetSchema(
          'count by(firmware_version) (ceph_hardware_storage_capacity_bytes{firmware_version!="unknown"})',
          legendFormat='{{firmware_version}}',
          instant=true
        ))
        + {
          fieldConfig+: {
            defaults+: {
              color+: { mode: 'palette-classic' },
            },
          },
          options+: {
            pieType: 'donut',
            legend+: { showLegend: true, placement: 'right' },
            displayLabels: ['value'],
          },
        },

        // Hosts
        $.addStatPanel(
          title='Hosts',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 4, w: 2, x: 0, y: 5 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'count(count by(hostname) (ceph_hardware_health))',
          legendFormat='Hosts'
        ))
        + { description: 'Total hosts with hardware monitoring' },

        // Drives
        $.addStatPanel(
          title='Drives',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 4, w: 2, x: 2, y: 5 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'count(ceph_hardware_storage_capacity_bytes)',
          legendFormat='Drives'
        ))
        + { description: 'Total storage drives detected' },

        // NVMe Temp
        $.addGaugePanel(
          title='NVMe Temp',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 4, y: 5 },
          min=0,
          max=85,
        )
        .addThresholds([
          { color: 'green' },
          { color: 'yellow', value: 75 },
          { color: 'red', value: 80 },
        ])
        .addTarget($.addTargetSchema(
          'max(ceph_hardware_temperature_celsius{sensor_name=~"NVME.*_TEMP"})',
          legendFormat='NVMe'
        ))
        + { description: 'Highest NVMe drive temperature across all hosts' },
      ] },

      // Row 2: Host Overview
      $.addRowSchema(true, true, 'Host Overview: $hostname') + { gridPos: { x: 0, y: 1, w: 24, h: 1 }, panels: [
        // Power
        $.addStatPanel(
          title='Power',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 3, w: 2, x: 0, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_health{hostname=~"$hostname",category="power"})',
          legendFormat='Power'
        ))
        + {
          description: 'Power supply health status',
          fieldConfig: {
            defaults: {
              mappings: [
                { type: 'value', options: { '0': { color: 'green', index: 0, text: 'OK' } } },
                { type: 'range', options: { from: 1, to: 9999, result: { color: 'red', index: 1, text: 'NOT OK' } } },
              ],
              noValue: 'N/A',
              thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: 'red', value: 1 }] },
            },
          },
          options+: { colorMode: 'background_solid' },
        },

        // MB Temperature
        $.addStatPanel(
          title='MB Temperature (max)',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 2, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~".*MB_TEMP.*"})',
          legendFormat='Motherboard'
        ))
        + {
          description: 'Highest motherboard temperature',
          fieldConfig+: { defaults+: { thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: '#EAB839', value: 70 }, { color: 'red', value: 80 }] } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },

        // CPU Temperature
        $.addStatPanel(
          title='CPU Temperature',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 5, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'avg(ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~".*CPU_TEMP"})',
          legendFormat='CPU'
        ))
        + {
          description: 'Average CPU temperature',
          fieldConfig+: { defaults+: { max: 85, min: 0, thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: '#EAB839', value: 80 }, { color: 'semi-dark-red', value: 90 }] } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },

        // DIMM Temperature
        $.addStatPanel(
          title='DIMM Temperature (max)',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 8, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~".*DIMM.*_TEMP"})',
          legendFormat='DIMM'
        ))
        + {
          description: 'Highest DIMM temperature',
          fieldConfig+: { defaults+: { max: 88, min: 0, thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: 'yellow', value: 70 }, { color: 'semi-dark-red', value: 80 }] } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },

        // PSU Fans
        $.addStatPanel(
          title='PSU Fans',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 11, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'count(ceph_hardware_fan_rpm{hostname=~"$hostname", fan_name=~"PSU.*"})',
          legendFormat='PSU Fans'
        ))
        + {
          description: 'Number of PSU fans detected',
          options+: { colorMode: 'none', graphMode: 'none' },
        },

        // AVG PSU Temperature
        $.addStatPanel(
          title='AVG PSU Temperature',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 14, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'avg(ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~"PSU.*_TEMP.*"})',
          legendFormat='PSU'
        ))
        + {
          description: 'Average power supply temperature',
          fieldConfig+: { defaults+: { color: { fixedColor: 'blue', mode: 'fixed' }, max: 100, min: 0, thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: 'yellow', value: 80 }, { color: 'semi-dark-red', value: 90 }] } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },

        // NVMe Drives
        $.addStatPanel(
          title='NVMe Drives',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 17, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'count(ceph_hardware_storage_capacity_bytes{hostname=~"$hostname", protocol="NVMe"})',
          legendFormat='NVMe'
        ))
        + {
          description: 'Number of NVMe drives detected',
          fieldConfig+: { defaults+: { color: { fixedColor: 'blue', mode: 'thresholds' }, thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: '#EAB839', value: 70 }, { color: 'semi-dark-red', value: 78 }] } } },
          options+: { colorMode: 'none', graphMode: 'none' },
        },

        // NVMe Temperature
        $.addStatPanel(
          title='NVMe Temperature (max)',
          unit='celsius',
          datasource='$datasource',
          gridPosition={ h: 4, w: 3, x: 20, y: 2 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~"NVME.*"})',
          legendFormat='NVMe'
        ))
        + {
          description: 'Highest NVMe drive temperature',
          fieldConfig+: { defaults+: { color: { fixedColor: 'blue', mode: 'thresholds' }, max: 85, min: 0, thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: '#EAB839', value: 70 }, { color: 'semi-dark-red', value: 78 }] } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },

        // Network
        $.addStatPanel(
          title='Network',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 3, w: 2, x: 0, y: 5 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_health{hostname=~"$hostname",category="network"})',
          legendFormat='Network'
        ))
        + {
          description: 'Network adapter health status',
          fieldConfig: {
            defaults: {
              mappings: [
                { type: 'value', options: { '0': { color: 'green', index: 0, text: 'OK' } } },
                { type: 'range', options: { from: 1, to: 9999, result: { color: 'red', index: 1, text: 'NOT OK' } } },
              ],
              noValue: 'N/A',
              thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: 'red', value: 1 }] },
            },
          },
          options+: { colorMode: 'background_solid' },
        },

        // Cooling
        $.addStatPanel(
          title='Cooling',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 3, w: 2, x: 0, y: 8 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_health{hostname=~"$hostname",category="fans"})',
          legendFormat='Cooling'
        ))
        + {
          description: 'Cooling fans health status',
          fieldConfig: {
            defaults: {
              mappings: [
                { type: 'value', options: { '0': { color: 'green', index: 0, text: 'OK' } } },
                { type: 'range', options: { from: 1, to: 9999, result: { color: 'red', index: 1, text: 'NOT OK' } } },
              ],
              noValue: 'N/A',
              thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: 'red', value: 1 }] },
            },
          },
          options+: { colorMode: 'background_solid' },
        },

        // Drives
        $.addStatPanel(
          title='Drives',
          unit='short',
          datasource='$datasource',
          gridPosition={ h: 3, w: 2, x: 0, y: 11 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'max(ceph_hardware_health{hostname=~"$hostname",category="storage"})',
          legendFormat='Drives'
        ))
        + {
          description: 'Storage drives health status',
          fieldConfig: {
            defaults: {
              mappings: [
                { type: 'value', options: { '0': { color: 'green', index: 0, text: 'OK' } } },
                { type: 'range', options: { from: 1, to: 9999, result: { color: 'red', index: 1, text: 'NOT OK' } } },
              ],
              noValue: 'N/A',
              thresholds: { mode: 'absolute', steps: [{ color: 'green' }, { color: 'red', value: 1 }] },
            },
          },
          options+: { colorMode: 'background_solid' },
        },

        // Device List
        $.addTableExtended(
          title='Device List',
          datasource='$datasource',
          gridPosition={ h: 8, w: 11, x: 2, y: 6 },
          options={
            footer: { show: false },
            showHeader: true,
          },
          custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
          thresholds={
            mode: 'absolute',
            steps: [{ color: 'green', value: null }],
          },
        )
        .addTarget($.addTargetSchema(
          'ceph_hardware_storage_capacity_bytes{hostname=~"$hostname"}',
          legendFormat='',
          instant=true
        ) + { format: 'table' })
        .addTransformations([
          {
            id: 'organize',
            options: {
              excludeByName: {
                Time: true,
                Value: true,
                __name__: true,
                cluster: true,
                hostname: true,
                job: true,
                instance: true,
              },
              renameByName: {
                device: 'Device Name',
                model: 'Drive Model',
                protocol: 'Protocol',
              },
            },
          },
        ]),

        // Platform Firmware
        $.addTableExtended(
          title='Platform Firmware',
          datasource='$datasource',
          gridPosition={ h: 8, w: 11, x: 13, y: 6 },
          options={
            footer: { show: false },
            showHeader: true,
          },
          custom={ align: 'auto', cellOptions: { type: 'auto' }, filterable: false, inspect: false },
          thresholds={
            mode: 'absolute',
            steps: [{ color: 'green', value: null }],
          },
        )
        .addTarget($.addTargetSchema(
          'ceph_hardware_firmware_info{hostname=~"$hostname"}',
          legendFormat='',
          instant=true
        ) + { format: 'table' })
        .addTransformations([
          {
            id: 'organize',
            options: {
              excludeByName: {
                Time: true,
                Value: true,
                __name__: true,
                cluster: true,
                hostname: true,
                job: true,
                instance: true,
              },
              renameByName: {
                component: 'Component',
                version: 'Version',
              },
            },
          },
        ]),
      ] },

      // Row 3: FAN Speeds
      $.addRowSchema(true, true, 'FAN Speeds: $hostname') + { gridPos: { x: 0, y: 2, w: 24, h: 1 }, panels: [
        // Repeating panel
        $.addStatPanel(
          title='FAN: $fan_speeds (RPM)',
          unit='locale',
          datasource='$datasource',
          gridPosition={ h: 5, w: 4, x: 0, y: 3 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'ceph_hardware_fan_rpm{hostname=~"$hostname",fan_name=~"$fan_speeds"}',
          legendFormat='{{fan_name}}'
        ))
        + {
          fieldConfig+: { defaults+: { color: { fixedColor: 'blue', mode: 'fixed' } } },
          options+: { colorMode: 'none', graphMode: 'area' },
          repeat: 'fan_speeds',
          repeatDirection: 'h',
          maxPerRow: 6,
        },

        // PSU1
        $.addStatPanel(
          title='PSU1 Fan Speed (RPM)',
          unit='locale',
          datasource='$datasource',
          gridPosition={ h: 5, w: 4, x: 0, y: 8 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'ceph_hardware_fan_rpm{hostname=~"$hostname",fan_name=~"PSU1.*"}',
          legendFormat='{{fan_name}}'
        ))
        + {
          fieldConfig+: { defaults+: { color: { fixedColor: 'blue', mode: 'fixed' } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },

        // PSU2
        $.addStatPanel(
          title='PSU2 Fan Speed (RPM)',
          unit='locale',
          datasource='$datasource',
          gridPosition={ h: 5, w: 4, x: 4, y: 8 },
          reducerFunction='last',
          noValue='N/A',
        ).addTarget($.addTargetSchema(
          'ceph_hardware_fan_rpm{hostname=~"$hostname",fan_name=~"PSU2.*"}',
          legendFormat='{{fan_name}}'
        ))
        + {
          fieldConfig+: { defaults+: { color: { fixedColor: 'blue', mode: 'fixed' } } },
          options+: { colorMode: 'none', graphMode: 'area' },
        },
      ] },

      // Row 4: Temperature History
      $.addRowSchema(true, true, 'Temperature History: $hostname') + { gridPos: { x: 0, y: 3, w: 24, h: 1 }, panels: [
        // CPU Temperature
        g.graphPanel.new(
          title='CPU Temperature',
          datasource='$datasource',
          format='celsius',
        ).addTarget(
          g.prometheus.target(
            'ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~".*CPU_TEMP"}',
            legendFormat='{{sensor_name}}'
          )
        ).addTarget(
          g.prometheus.target('vector(100)', legendFormat='Critical')
        )
        + {
          gridPos: { x: 0, y: 4, w: 12, h: 8 },
          yaxes: [{ min: 0 }, {}],
          seriesOverrides: [
            { alias: 'Critical', color: 'dark-red', dashes: true, fill: 0 },
          ],
        },

        // DIMM Temperatures
        g.graphPanel.new(
          title='DIMM Temperatures',
          datasource='$datasource',
          format='celsius',
        ).addTarget(
          g.prometheus.target(
            'ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~".*DIMM.*_TEMP"}',
            legendFormat='{{sensor_name}}'
          )
        ).addTarget(
          g.prometheus.target('vector(88)', legendFormat='Critical')
        )
        + {
          gridPos: { x: 12, y: 4, w: 12, h: 8 },
          yaxes: [{ min: 0 }, {}],
          seriesOverrides: [
            { alias: 'Critical', color: 'dark-red', dashes: true, fill: 0 },
          ],
        },

        // Motherboard Temperatures
        g.graphPanel.new(
          title='Motherboard Temperatures',
          datasource='$datasource',
          format='celsius',
        ).addTarget(
          g.prometheus.target(
            'ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~".*MB_TEMP.*"}',
            legendFormat='{{sensor_name}}'
          )
        ).addTarget(
          g.prometheus.target('vector(85)', legendFormat='Critical')
        )
        + {
          gridPos: { x: 0, y: 12, w: 12, h: 8 },
          yaxes: [{ min: 0 }, {}],
          seriesOverrides: [
            { alias: 'Critical', color: 'dark-red', dashes: true, fill: 0 },
          ],
        },

        // NVMe Temperatures
        g.graphPanel.new(
          title='NVMe Temperatures',
          datasource='$datasource',
          format='celsius',
        ).addTarget(
          g.prometheus.target(
            'ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~"NVME.*_TEMP"}',
            legendFormat='{{sensor_name}}'
          )
        ).addTarget(
          g.prometheus.target('vector(85)', legendFormat='Critical')
        )
        + {
          gridPos: { x: 12, y: 12, w: 12, h: 8 },
          yaxes: [{ min: 0 }, {}],
          seriesOverrides: [
            { alias: 'Critical', color: 'dark-red', dashes: true, fill: 0 },
          ],
        },

        // PSU Temperatures
        g.graphPanel.new(
          title='PSU Temperatures',
          datasource='$datasource',
          format='celsius',
        ).addTarget(
          g.prometheus.target(
            'ceph_hardware_temperature_celsius{hostname=~"$hostname", sensor_name=~"PSU.*_TEMP.*"}',
            legendFormat='{{sensor_name}}'
          )
        ).addTarget(
          g.prometheus.target('vector(65)', legendFormat='Critical')
        )
        + {
          gridPos: { x: 0, y: 20, w: 12, h: 8 },
          yaxes: [{ min: 0 }, {}],
          seriesOverrides: [
            { alias: 'Critical', color: 'dark-red', dashes: true, fill: 0 },
          ],
        },
      ] },

      // Row 5: FAN Speed History
      $.addRowSchema(true, true, 'FAN Speed History: $hostname') + { gridPos: { x: 0, y: 4, w: 24, h: 1 }, panels: [
        // AVG PSU Fan Speed
        g.graphPanel.new(
          title='AVG PSU Fan Speed',
          datasource='$datasource',
          format='locale',
          fill=1,
        ).addTarget(
          g.prometheus.target(
            'avg(ceph_hardware_fan_rpm{hostname=~"$hostname", fan_name=~"PSU.*"})',
            legendFormat='PSU Fans'
          )
        )
        + {
          gridPos: { x: 0, y: 5, w: 12, h: 8 },
          description: 'AVG across both PSUs',
        },

        // AVG Cooling Fan RPMs
        g.graphPanel.new(
          title='AVG Cooling Fan RPMs',
          datasource='$datasource',
          format='locale',
          fill=1,
        ).addTarget(
          g.prometheus.target(
            'avg(ceph_hardware_fan_rpm{hostname=~"$hostname", fan_name!~"PSU.*"})',
            legendFormat='System Fans'
          )
        )
        + {
          gridPos: { x: 12, y: 5, w: 12, h: 8 },
        },
      ] },
    ]),
}
