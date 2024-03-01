import { Component, Input, ViewChild, OnChanges, SimpleChanges } from '@angular/core';

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
export class DashboardAreaChartComponent implements OnChanges {
  @ViewChild(BaseChartDirective) chart: BaseChartDirective;

  @Input()
  chartTitle: string;
  @Input()
  maxValue?: number;
  @Input()
  dataUnits: string;
  @Input()
  dataArray?: Array<Array<[number, string]>>; // Array of query results
  @Input()
  labelsArray?: string[] = []; // Array of chart labels
  @Input()
  decimals?: number = 1;

  currentDataUnits: string;
  currentData: number;
  maxConvertedValue?: number;
  maxConvertedValueUnits?: string;

  chartDataUnits: string;
  chartData: any = { dataset: [] };
  options: any = {};
  currentChartData: any = {};

  chartColors: any[] = [
    [
      this.cssHelper.propertyValue('chart-color-strong-blue'),
      this.cssHelper.propertyValue('chart-color-translucent-blue')
    ],
    [
      this.cssHelper.propertyValue('chart-color-orange'),
      this.cssHelper.propertyValue('chart-color-translucent-orange')
    ],
    [
      this.cssHelper.propertyValue('chart-color-green'),
      this.cssHelper.propertyValue('chart-color-translucent-green')
    ],
    [
      this.cssHelper.propertyValue('chart-color-cyan'),
      this.cssHelper.propertyValue('chart-color-translucent-cyan')
    ],
    [
      this.cssHelper.propertyValue('chart-color-purple'),
      this.cssHelper.propertyValue('chart-color-translucent-purple')
    ],
    [
      this.cssHelper.propertyValue('chart-color-red'),
      this.cssHelper.propertyValue('chart-color-translucent-red')
    ]
  ];

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

  ngOnChanges(changes: SimpleChanges): void {
    this.updateChartData(changes);
  }

  ngAfterViewInit() {
    this.updateChartData(null);
  }

  private updateChartData(changes: SimpleChanges): void {
    for (let index = 0; index < this.labelsArray.length; index++) {
      const colorIndex = index % this.chartColors.length;
      this.chartData.dataset[index] = {
        label: '',
        data: [],
        tension: 0.2,
        pointBackgroundColor: this.chartColors[colorIndex][0],
        backgroundColor: this.chartColors[colorIndex][1],
        borderColor: this.chartColors[colorIndex][0],
        borderWidth: 1,
        fill: {
          target: 'origin'
        }
      };
      this.chartData.dataset[index].label = this.labelsArray[index];
    }

    this.setChartTicks();

    if (this.dataArray && this.dataArray.length && this.dataArray[0] && this.dataArray[0].length) {
      this.dataArray = changes?.dataArray?.currentValue || this.dataArray;
      this.currentChartData = this.chartData;
      for (let index = 0; index < this.dataArray.length; index++) {
        this.chartData.dataset[index].data = this.formatData(this.dataArray[index]);
        let currentDataValue = this.dataArray[index][this.dataArray[index].length - 1]
          ? this.dataArray[index][this.dataArray[index].length - 1][1]
          : 0;
        if (currentDataValue) {
          [
            this.currentChartData.dataset[index]['currentData'],
            this.currentChartData.dataset[index]['currentDataUnits']
          ] = this.convertUnits(currentDataValue).split(' ');
          [this.maxConvertedValue, this.maxConvertedValueUnits] = this.convertUnits(
            this.maxValue
          ).split(' ');
        }
      }
    }

    if (this.chart) {
      this.chart.chart.update();
    }
  }

  private formatData(array: Array<any>): any {
    let formattedData = {};
    formattedData = array?.map((data: any) => ({
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

    const allDataValues = this.dataArray.reduce((array: string[], data) => {
      return array.concat(data?.map((values: [number, string]) => values[1]));
    }, []);

    maxValue = Math.max(...allDataValues.map(Number));
    [maxValue, maxValueDataUnits] = this.convertUnits(maxValue).split(' ');

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
