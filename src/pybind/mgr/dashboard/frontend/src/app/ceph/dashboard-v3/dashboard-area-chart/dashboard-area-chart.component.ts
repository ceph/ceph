import { AfterViewInit, Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';

import { CssHelper } from '~/app/shared/classes/css-helper';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { BaseChartDirective, PluginServiceGlobalRegistrationAndOptions } from 'ng2-charts';
import { DimlessPipe } from '~/app/shared/pipes/dimless.pipe';

@Component({
  selector: 'cd-dashboard-area-chart',
  templateUrl: './dashboard-area-chart.component.html',
  styleUrls: ['./dashboard-area-chart.component.scss']
})
export class DashboardAreaChartComponent implements OnInit, OnChanges, AfterViewInit {
  @ViewChild(BaseChartDirective) chart: BaseChartDirective;

  @Input()
  chartTitle: string;
  @Input()
  maxValue?: any;
  @Input()
  dataUnits: string;
  @Input()
  data: any;
  @Input()
  data2?: any;
  @Input()
  label: any;
  @Input()
  label2?: any;

  currentDataUnits: string;
  currentData: number;
  currentDataUnits2?: string;
  currentData2?: number;

  chartData: any = {
    dataset: [
      {
        label: '',
        data: [{ x: 0, y: 0 }],
        tension: 0,
        pointBackgroundColor: this.cssHelper.propertyValue('chart-color-strong-blue'),
        backgroundColor: this.cssHelper.propertyValue('chart-color-translucent-blue'),
        borderColor: this.cssHelper.propertyValue('chart-color-strong-blue')
      },
      {
        label: '',
        data: [],
        tension: 0,
        pointBackgroundColor: this.cssHelper.propertyValue('chart-color-orange'),
        backgroundColor: this.cssHelper.propertyValue('chart-color-yellow'),
        borderColor: this.cssHelper.propertyValue('chart-color-orange')
      }
    ]
  };

  options: any = {
    responsive: true,
    maintainAspectRatio: false,
    elements: {
      point: {
        radius: 0
      }
    },
    legend: {
      display: false
    },
    tooltips: {
      intersect: false,
      displayColors: true,
      backgroundColor: this.cssHelper.propertyValue('chart-color-tooltip-background'),
      callbacks: {
        title: function (tooltipItem: any): any {
          return tooltipItem[0].xLabel;
        }
      }
    },
    hover: {
      intersect: false
    },
    scales: {
      xAxes: [
        {
          display: false,
          type: 'time',
          gridLines: {
            display: false
          },
          time: {
            tooltipFormat: 'YYYY/MM/DD hh:mm:ss'
          }
        }
      ],
      yAxes: [
        {
          gridLines: {
            display: false
          },
          ticks: {
            beginAtZero: true,
            maxTicksLimit: 3,
            callback: (value: any) => {
              if (value === 0) {
                return null;
              }
              return this.fillString(this.convertUnits(value));
            }
          }
        }
      ]
    },
    plugins: {
      borderArea: true,
      chartAreaBorder: {
        borderColor: this.cssHelper.propertyValue('chart-color-slight-dark-gray'),
        borderWidth: 2
      }
    }
  };

  public chartAreaBorderPlugin: PluginServiceGlobalRegistrationAndOptions[] = [
    {
      beforeDraw(chart: Chart) {
        if (!chart.options.plugins.borderArea) {
          return;
        }
        const {
          ctx,
          chartArea: { left, top, right, bottom }
        } = chart;
        ctx.save();
        ctx.strokeStyle = chart.options.plugins.chartAreaBorder.borderColor;
        ctx.lineWidth = chart.options.plugins.chartAreaBorder.borderWidth;
        ctx.setLineDash(chart.options.plugins.chartAreaBorder.borderDash || []);
        ctx.lineDashOffset = chart.options.plugins.chartAreaBorder.borderDashOffset;
        ctx.strokeRect(left, top, right - left - 1, bottom);
        ctx.restore();
      }
    }
  ];

  constructor(
    private cssHelper: CssHelper,
    private dimlessBinary: DimlessBinaryPipe,
    private dimlessBinaryPerSecond: DimlessBinaryPerSecondPipe,
    private dimlessPipe: DimlessPipe,
    private formatter: FormatterService
  ) {}

  ngOnInit(): void {
    this.currentData = Number(
      this.chartData.dataset[0].data[this.chartData.dataset[0].data.length - 1].y
    );
    if (this.data2) {
      this.currentData2 = Number(
        this.chartData.dataset[1].data[this.chartData.dataset[1].data.length - 1].y
      );
    }
  }

  ngOnChanges(): void {
    if (this.data) {
      this.setChartTicks();
      this.chartData.dataset[0].data = this.formatData(this.data);
      this.chartData.dataset[0].label = this.label;
      [this.currentData, this.currentDataUnits] = this.convertUnits(
        this.data[this.data.length - 1][1]
      ).split(' ');
    }
    if (this.data2) {
      this.chartData.dataset[1].data = this.formatData(this.data2);
      this.chartData.dataset[1].label = this.label2;
      [this.currentData2, this.currentDataUnits2] = this.convertUnits(
        this.data2[this.data2.length - 1][1]
      ).split(' ');
    }
  }

  ngAfterViewInit(): void {
    if (this.data) {
      this.setChartTicks();
    }
  }

  private formatData(array: Array<any>): any {
    let formattedData = {};
    formattedData = array.map((data: any) => ({
      x: data[0] * 1000,
      y: Number(this.convertUnits(data[1]).replace(/[^\d,.]+/g, ''))
    }));
    return formattedData;
  }

  private convertUnits(data: any): any {
    let dataWithUnits: string;
    if (this.dataUnits === 'bytes') {
      dataWithUnits = this.dimlessBinary.transform(data);
    } else if (this.dataUnits === 'bytesPerSecond') {
      dataWithUnits = this.dimlessBinaryPerSecond.transform(data);
    } else if (this.dataUnits === 'ms') {
      dataWithUnits = this.formatter.format_number(data, 1000, ['ms', 's']);
    } else {
      dataWithUnits = this.dimlessPipe.transform(data);
    }
    return dataWithUnits;
  }

  private fillString(str: string): string {
    let maxNumberOfChar: number = 8;
    let numberOfChars: number = str.length;
    if (str.length < 4) {
      maxNumberOfChar = 11;
    }
    for (; numberOfChars < maxNumberOfChar; numberOfChars++) {
      str = '\u00A0' + str;
    }
    return str + '\u00A0\u00A0';
  }

  private setChartTicks() {
    if (this.chart && this.maxValue) {
      let [maxValue, maxValueDataUnits] = this.convertUnits(this.maxValue).split(' ');
      this.chart.chart.options.scales.yAxes[0].ticks.suggestedMax = maxValue;
      this.chart.chart.options.scales.yAxes[0].ticks.suggestedMin = 0;
      this.chart.chart.options.scales.yAxes[0].ticks.stepSize = Number((maxValue / 2).toFixed(0));
      this.chart.chart.options.scales.yAxes[0].ticks.callback = (value: any) => {
        if (value === 0) {
          return null;
        }
        return this.fillString(`${value} ${maxValueDataUnits}`);
      };
      this.chart.chart.update();
    } else if (this.chart && this.data) {
      let maxValue = 0,
        maxValueDataUnits = '';
      let maxValueData = Math.max(...this.data.map((values: any) => values[1]));
      if (this.data2) {
        var maxValueData2 = Math.max(...this.data2.map((values: any) => values[1]));
        [maxValue, maxValueDataUnits] = this.convertUnits(
          Math.max(maxValueData, maxValueData2)
        ).split(' ');
      } else {
        [maxValue, maxValueDataUnits] = this.convertUnits(Math.max(maxValueData)).split(' ');
      }

      this.chart.chart.options.scales.yAxes[0].ticks.suggestedMax = maxValue * 1.2;
      this.chart.chart.options.scales.yAxes[0].ticks.suggestedMin = 0;
      this.chart.chart.options.scales.yAxes[0].ticks.stepSize = Number(
        ((maxValue * 1.2) / 2).toFixed(0)
      );
      this.chart.chart.options.scales.yAxes[0].ticks.callback = (value: any) => {
        if (value === 0) {
          return null;
        }
        if (!maxValueDataUnits) {
          return this.fillString(`${value}`);
        }
        return this.fillString(`${value} ${maxValueDataUnits}`);
      };
      this.chart.chart.update();
    }
  }
}
