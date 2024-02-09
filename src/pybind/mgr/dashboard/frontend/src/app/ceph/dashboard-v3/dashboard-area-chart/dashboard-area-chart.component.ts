import { AfterViewInit, Component, Input, OnChanges, ViewChild } from '@angular/core';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { BaseChartDirective } from 'ng2-charts';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';
import { NumberFormatterService } from '~/app/shared/services/number-formatter.service';
import 'chartjs-adapter-moment';

@Component({
  selector: 'cd-dashboard-area-chart',
  templateUrl: './dashboard-area-chart.component.html',
  styleUrls: ['./dashboard-area-chart.component.scss']
})
export class DashboardAreaChartComponent implements OnChanges, AfterViewInit {
  @ViewChild(BaseChartDirective) chart: BaseChartDirective;

  @Input()
  chartTitle: string;
  @Input()
  maxValue?: number;
  @Input()
  dataUnits: string;
  @Input()
  data: Array<[number, string]>;
  @Input()
  data2?: Array<[number, string]>;
  @Input()
  label: string;
  @Input()
  label2?: string;
  @Input()
  decimals?: number = 1;

  currentDataUnits: string;
  currentData: number;
  currentDataUnits2?: string;
  currentData2?: number;
  maxConvertedValue?: number;
  maxConvertedValueUnits?: string;

  chartDataUnits: string;
  chartData: any;
  options: any;

  public chartAreaBorderPlugin: any[] = [
    {
      beforeDraw(chart: any) {
        if (!chart.options.plugins.borderArea) {
          return;
        }
        const {
          ctx,
          chartArea: { left, top, width, height }
        } = chart;
        ctx.save();
        ctx.strokeStyle = chart.options.plugins.chartAreaBorder.borderColor;
        ctx.lineWidth = chart.options.plugins.chartAreaBorder.borderWidth;
        ctx.setLineDash(chart.options.plugins.chartAreaBorder.borderDash || []);
        ctx.lineDashOffset = chart.options.plugins.chartAreaBorder.borderDashOffset;
        ctx.strokeRect(left, top, width, height);
        ctx.restore();
      }
    }
  ];

  constructor(
    private cssHelper: CssHelper,
    private dimlessBinary: DimlessBinaryPipe,
    private dimlessBinaryPerSecond: DimlessBinaryPerSecondPipe,
    private dimlessPipe: DimlessPipe,
    private formatter: FormatterService,
    private numberFormatter: NumberFormatterService
  ) {
    this.chartData = {
      dataset: [
        {
          label: '',
          data: [{ x: 0, y: 0 }],
          tension: 0.2,
          pointBackgroundColor: this.cssHelper.propertyValue('chart-color-strong-blue'),
          backgroundColor: this.cssHelper.propertyValue('chart-color-translucent-blue'),
          borderColor: this.cssHelper.propertyValue('chart-color-strong-blue'),
          borderWidth: 1,
          fill: {
            target: 'origin'
          }
        },
        {
          label: '',
          data: [],
          tension: 0.2,
          pointBackgroundColor: this.cssHelper.propertyValue('chart-color-orange'),
          backgroundColor: this.cssHelper.propertyValue('chart-color-translucent-yellow'),
          borderColor: this.cssHelper.propertyValue('chart-color-orange'),
          borderWidth: 1,
          fill: {
            target: 'origin'
          }
        }
      ]
    };

    this.options = {
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          mode: 'index',
          external: function (tooltipModel: any) {
            tooltipModel.tooltip.x = 10;
            tooltipModel.tooltip.y = 0;
          }.bind(this),
          intersect: false,
          displayColors: true,
          backgroundColor: this.cssHelper.propertyValue('chart-color-tooltip-background'),
          callbacks: {
            title: function (tooltipItem: any): any {
              return tooltipItem[0].xLabel;
            },
            label: (context: any) => {
              return (
                ' ' +
                context.dataset.label +
                ' - ' +
                context.formattedValue +
                ' ' +
                this.chartDataUnits
              );
            }
          }
        },
        borderArea: true,
        chartAreaBorder: {
          borderColor: this.cssHelper.propertyValue('chart-color-slight-dark-gray'),
          borderWidth: 1
        }
      },
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      elements: {
        point: {
          radius: 0
        }
      },
      hover: {
        intersect: false
      },
      scales: {
        x: {
          display: false,
          type: 'time',
          grid: {
            display: false
          },
          time: {
            tooltipFormat: 'DD/MM/YYYY - HH:mm:ss'
          }
        },
        y: {
          afterFit: (scaleInstance: any) => (scaleInstance.width = 100),
          grid: {
            display: false
          },
          beginAtZero: true,
          ticks: {
            maxTicksLimit: 4
          }
        }
      }
    };
  }

  ngOnChanges(): void {
    this.updateChartData();
  }

  ngAfterViewInit(): void {
    this.updateChartData();
  }

  private updateChartData(): void {
    this.chartData.dataset[0].label = this.label;
    this.chartData.dataset[1].label = this.label2;
    this.setChartTicks();
    if (this.data) {
      this.chartData.dataset[0].data = this.formatData(this.data);
      [this.currentData, this.currentDataUnits] = this.convertUnits(
        this.data[this.data.length - 1][1]
      ).split(' ');
      [this.maxConvertedValue, this.maxConvertedValueUnits] = this.convertUnits(
        this.maxValue
      ).split(' ');
    }
    if (this.data2) {
      this.chartData.dataset[1].data = this.formatData(this.data2);
      [this.currentData2, this.currentDataUnits2] = this.convertUnits(
        this.data2[this.data2.length - 1][1]
      ).split(' ');
    }
    if (this.chart) {
      this.chart.chart.update();
    }
  }

  private formatData(array: Array<any>): any {
    let formattedData = {};
    formattedData = array.map((data: any) => ({
      x: data[0] * 1000,
      y: Number(this.convertToChartDataUnits(data[1]).replace(/[^\d,.]+/g, ''))
    }));
    return formattedData;
  }

  private convertToChartDataUnits(data: any): any {
    let dataWithUnits: string = '';
    if (this.chartDataUnits !== null) {
      if (this.dataUnits === 'B') {
        dataWithUnits = this.numberFormatter.formatBytesFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits,
          this.decimals
        );
      } else if (this.dataUnits === 'B/s') {
        dataWithUnits = this.numberFormatter.formatBytesPerSecondFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits,
          this.decimals
        );
      } else if (this.dataUnits === 'ms') {
        dataWithUnits = this.numberFormatter.formatSecondsFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits,
          this.decimals
        );
      } else {
        dataWithUnits = this.numberFormatter.formatUnitlessFromTo(
          data,
          this.dataUnits,
          this.chartDataUnits,
          this.decimals
        );
      }
    }
    return dataWithUnits;
  }

  private convertUnits(data: any): any {
    let dataWithUnits: string = '';
    if (this.dataUnits === 'B') {
      dataWithUnits = this.dimlessBinary.transform(data, this.decimals);
    } else if (this.dataUnits === 'B/s') {
      dataWithUnits = this.dimlessBinaryPerSecond.transform(data, this.decimals);
    } else if (this.dataUnits === 'ms') {
      dataWithUnits = this.formatter.format_number(data, 1000, ['ms', 's'], this.decimals);
    } else {
      dataWithUnits = this.dimlessPipe.transform(data, this.decimals);
    }
    return dataWithUnits;
  }

  private setChartTicks() {
    if (!this.chart) {
      this.chartDataUnits = '';
      return;
    }

    let maxValue = 0;
    let maxValueDataUnits = '';

    if (this.data) {
      let maxValueData = Math.max(...this.data.map((values: any) => values[1]));
      if (this.data2) {
        let maxValueData2 = Math.max(...this.data2.map((values: any) => values[1]));
        maxValue = Math.max(maxValueData, maxValueData2);
      } else {
        maxValue = maxValueData;
      }
      [maxValue, maxValueDataUnits] = this.convertUnits(maxValue).split(' ');
    }

    const yAxesTicks = this.chart.chart.options.scales.y;
    yAxesTicks.ticks.callback = (value: any) => {
      if (value === 0) {
        return null;
      }
      if (!maxValueDataUnits) {
        return `${value}`;
      }
      return `${value} ${maxValueDataUnits}`;
    };
    this.chartDataUnits = maxValueDataUnits || '';
    this.chart.chart.update();
  }
}
