import { Component, Input, OnChanges, OnInit } from '@angular/core';

import * as Chart from 'chart.js';
import _ from 'lodash';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';

@Component({
  selector: 'cd-dashboard-pie',
  templateUrl: './dashboard-pie.component.html',
  styleUrls: ['./dashboard-pie.component.scss']
})
export class DashboardPieComponent implements OnChanges, OnInit {
  @Input()
  data: any;
  @Input()
  highThreshold: number;
  @Input()
  lowThreshold: number;

  color: string;

  chartConfig: any;

  public doughnutChartPlugins: any[] = [
    {
      id: 'center_text',
      beforeDraw(chart: any) {
        const cssHelper = new CssHelper();
        const defaultFontFamily = 'Helvetica Neue, Helvetica, Arial, sans-serif';
        Chart.defaults.font.family = defaultFontFamily;
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

  constructor(private cssHelper: CssHelper, private dimlessBinary: DimlessBinaryPipe) {
    this.chartConfig = {
      chartType: 'doughnut',
      labels: ['', '', ''],
      dataset: [
        {
          label: null,
          backgroundColor: [
            this.cssHelper.propertyValue('chart-color-light-gray'),
            this.cssHelper.propertyValue('chart-color-slight-dark-gray'),
            this.cssHelper.propertyValue('chart-color-dark-gray')
          ]
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
        cutout: '70%',
        events: ['click', 'mouseout', 'touchstart'],
        aspectRatio: 2,
        plugins: {
          center_text: true,
          legend: {
            display: true,
            position: 'right',
            labels: {
              boxWidth: 10,
              usePointStyle: false,
              generateLabels: (chart: any) => {
                let labels = chart.data.labels.slice(0, this.chartConfig.labels.length);
                labels[0] = {
                  text: $localize`Used: ${chart.data.datasets[1].data[2]}`,
                  fillStyle: chart.data.datasets[1].backgroundColor[0],
                  strokeStyle: chart.data.datasets[1].backgroundColor[0]
                };
                labels[1] = {
                  text: $localize`Warning: ${chart.data.datasets[0].data[0]}%`,
                  fillStyle: chart.data.datasets[0].backgroundColor[1],
                  strokeStyle: chart.data.datasets[0].backgroundColor[1]
                };
                labels[2] = {
                  text: $localize`Danger: ${
                    chart.data.datasets[0].data[0] + chart.data.datasets[0].data[1]
                  }%`,
                  fillStyle: chart.data.datasets[0].backgroundColor[2],
                  strokeStyle: chart.data.datasets[0].backgroundColor[2]
                };

                return labels;
              }
            }
          },
          tooltip: {
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
      }
    };
  }

  ngOnInit() {
    this.prepareRawUsage(this.chartConfig, this.data);
  }

  ngOnChanges() {
    this.prepareRawUsage(this.chartConfig, this.data);
  }

  private prepareRawUsage(chart: Record<string, any>, data: Record<string, any>) {
    const nearFullRatioPercent = this.lowThreshold * 100;
    const fullRatioPercent = this.highThreshold * 100;
    const percentAvailable = this.calcPercentage(data.max - data.current, data.max);
    const percentUsed = this.calcPercentage(data.current, data.max);
    if (percentUsed >= fullRatioPercent) {
      this.color = 'chart-color-red';
    } else if (percentUsed >= nearFullRatioPercent) {
      this.color = 'chart-color-yellow';
    } else {
      this.color = 'chart-color-blue';
    }

    chart.dataset[0].data = [
      Math.round(nearFullRatioPercent),
      Math.round(Math.abs(nearFullRatioPercent - fullRatioPercent)),
      Math.round(100 - fullRatioPercent)
    ];

    chart.dataset[1].data = [
      percentUsed,
      percentAvailable,
      this.dimlessBinary.transform(data.current)
    ];
    chart.dataset[1].backgroundColor[0] = this.cssHelper.propertyValue(this.color);

    chart.dataset[0].label = [`${percentUsed}%\nof ${this.dimlessBinary.transform(data.max)}`];
  }

  private calcPercentage(dividend: number, divisor: number) {
    if (!_.isNumber(dividend) || !_.isNumber(divisor) || divisor === 0) {
      return 0;
    }
    return Math.ceil((dividend / divisor) * 100 * 100) / 100;
  }
}
