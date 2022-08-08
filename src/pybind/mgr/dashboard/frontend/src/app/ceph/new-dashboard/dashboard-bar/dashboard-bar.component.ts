import { Component, EventEmitter, Input, OnChanges, OnInit, Output } from '@angular/core';

import * as Chart from 'chart.js';
import _ from 'lodash';
import { PluginServiceGlobalRegistrationAndOptions } from 'ng2-charts';

import { CssHelper } from '~/app/shared/classes/css-helper';

@Component({
  selector: 'cd-dashboard-bar',
  templateUrl: './dashboard-bar.component.html',
  styleUrls: ['./dashboard-bar.component.scss']
})
export class DashboardBarComponent implements OnChanges, OnInit {
  @Input()
  data: any;
  @Output()
  prepareFn = new EventEmitter();

  chartConfig: any = {
    chartType: 'doughnut',
    labels: ['', '', ''],
    dataset: [
      {
        label: null,
        backgroundColor: ['rgb(240, 240, 240)', 'rgb(215, 215, 215)', 'rgb(175, 175, 175)']
      },
      {
        label: null,
        borderWidth: 0,
        backgroundColor: [
          this.cssHelper.propertyValue('chart-color-blue'),
          this.cssHelper.propertyValue('chart-color-white')
        ]
      }
    ],
    options: {
      cutoutPercentage: 70,
      events: ['click', 'mouseout', 'touchstart'],
      legend: {
        display: true,
        position: 'right',
        labels: {
          boxWidth: 10,
          usePointStyle: false,
          generateLabels: (chart: any) => {
            const labels = { 0: {}, 1: {}, 2: {} };
            labels[0] = {
              text: $localize`Warning: ` + chart.data.datasets[0].data[0] + '%',
              fillStyle: chart.data.datasets[0].backgroundColor[1],
              strokeStyle: chart.data.datasets[0].backgroundColor[1]
            };
            labels[1] = {
              text:
                $localize`Danger: ` +
                (chart.data.datasets[0].data[0] + chart.data.datasets[0].data[1]) +
                '%',
              fillStyle: chart.data.datasets[0].backgroundColor[2],
              strokeStyle: chart.data.datasets[0].backgroundColor[2]
            };
            labels[2] = {
              text: $localize`Capacity: ` + chart.data.datasets[1].data[0] + '%',
              fillStyle: chart.data.datasets[1].backgroundColor[0],
              strokeStyle: chart.data.datasets[1].backgroundColor[0]
            };
            return labels;
          }
        }
      },
      plugins: {
        center_text: true
      },
      tooltips: {
        enabled: true,
        displayColors: false,
        backgroundColor: this.cssHelper.propertyValue('chart-color-tooltip-background'),
        cornerRadius: 0,
        bodyFontSize: 14,
        bodyFontStyle: '600',
        position: 'nearest',
        xPadding: 12,
        yPadding: 12,
        filter: (tooltipItem: any) => {
          return tooltipItem.datasetIndex === 1;
        },
        callbacks: {
          label: (item: Record<string, any>, data: Record<string, any>) => {
            let text = data.labels[item.index];
            if (!text.includes('%')) {
              text = `${text} (${data.datasets[item.datasetIndex].data[item.index]}%)`;
            }
            return text;
          }
        }
      },
      title: {
        display: false
      }
    }
  };

  public doughnutChartPlugins: PluginServiceGlobalRegistrationAndOptions[] = [
    {
      id: 'center_text',
      beforeDraw(chart: Chart) {
        const cssHelper = new CssHelper();
        const defaultFontFamily = 'Helvetica Neue, Helvetica, Arial, sans-serif';
        Chart.defaults.global.defaultFontFamily = defaultFontFamily;
        const ctx = chart.ctx;
        if (!chart.options.plugins.center_text || !chart.data.datasets[0].label) {
          return;
        }

        ctx.save();
        const label = chart.data.datasets[0].label[0].split('\n');

        const centerX = (chart.chartArea.left + chart.chartArea.right) / 2;
        const centerY = (chart.chartArea.top + chart.chartArea.bottom) / 2;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';

        ctx.font = `24px ${defaultFontFamily}`;
        ctx.fillText(label[0], centerX, centerY - 10);

        if (label.length > 1) {
          ctx.font = `14px ${defaultFontFamily}`;
          ctx.fillStyle = cssHelper.propertyValue('chart-color-center-text-description');
          ctx.fillText(label[1], centerX, centerY + 10);
        }
        ctx.restore();
      }
    }
  ];

  constructor(private cssHelper: CssHelper) {}

  ngOnInit() {
    this.prepareFn.emit([this.chartConfig, this.data]);
  }

  ngOnChanges() {
    this.prepareFn.emit([this.chartConfig, this.data]);
  }
}