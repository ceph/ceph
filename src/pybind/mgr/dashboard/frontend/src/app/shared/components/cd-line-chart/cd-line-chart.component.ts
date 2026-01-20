import { AfterViewInit, Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
// import { data } from './data';
import { LineChartOptions, ScaleTypes } from '@carbon/charts-angular';
import { NumberFormatterService } from '../../services/number-formatter.service';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { DimlessBinaryPerSecondPipe } from '../../pipes/dimless-binary-per-second.pipe';
import { DimlessPipe } from '../../pipes/dimless.pipe';
import { FormatterService } from '../../services/formatter.service';

@Component({
  selector: 'cd-line-chart',
  templateUrl: './cd-line-chart.component.html',
  styleUrls: ['./cd-line-chart.component.scss'],
  standalone: false
})
export class CdLineChartComponent implements OnInit, OnChanges, AfterViewInit {


  @Input() title: string; // default value to be removed
  @Input() scaleType: ScaleTypes = 'time' as ScaleTypes; // 'time' | 'linear' | 'log'
  @Input() legendEnabled?: boolean = true;
  @Input() toolbarEnabled?: boolean = true;
  @Input() tooltipEnabled?: boolean = true;
  @Input() height?: string = '400px';
  @Input() width?: string = '100%';
  // Optional custom label (e.g., <ng-template><div>Capacity</div></<ng-template>) (overrides default label).
  // @Input() customLabel?: TemplateRef<any> | string = '';
  @Input() decimals?: number = 2;
  @Input() dataUnits?: string = 'B'; // 'B', 'B/s', 'ms', 'unitless'
  @Input() chartDataArray: Array<Array<[number, string]>> = [];
  @Input() labelsArray?: string[] = [];

  chartDataUnits: string | null = null; // 'B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB', etc.

  

  options:LineChartOptions = {};
  data = Array<any>();

  constructor(
    private numberFormatter: NumberFormatterService,
    private dimlessBinary: DimlessBinaryPipe,
    private dimlessBinaryPerSecond: DimlessBinaryPerSecondPipe,
    private dimlessPipe: DimlessPipe,
    private formatter: FormatterService,
  ) {}
  ngAfterViewInit(): void {
    this.chartDataArray.forEach((chartData, index) => {
      this.data = this.data.concat(this.formatData(chartData, this.labelsArray[index]));
    });
  }
  ngOnChanges(changes: SimpleChanges): void {
    const changeArr = changes.chartDataArray.currentValue? changes.chartDataArray.currentValue: this.chartDataArray;
    changeArr.forEach((chartData, index) => {
      this.data = this.data.concat(this.formatData(chartData, this.labelsArray[index]));
    });
  }

  ngOnInit(): void {
    this.options = {
      title: this.title,
      axes: {
        left: {
          scaleType: 'linear' as ScaleTypes,
          mapsTo: 'value',
          title: this.title,
          ticks: {
            min:0,
            formatter: this.setChartTicks.bind(this)
          }
        },
        bottom: {
          scaleType: this.scaleType,
          mapsTo: 'property',
          title: 'Time'
        }
      },
      legend: {
        enabled: this.legendEnabled,
        clickable: false
      },
      toolbar: {
        enabled: this.toolbarEnabled
      },
      tooltip: {
        enabled: this.tooltipEnabled
      },
      height: this.height,
      width: this.width
    }

    // this.data = this.formatData(data);
    // let tempData = Array<any>();
    // data.forEach((chartData, index) => {
    //   this.data = this.data.concat(this.formatData(chartData, this.labelsArray[index]));
    // });
    this.chartDataArray.forEach((chartData, index) => {
      this.data = this.data.concat(this.formatData(chartData, this.labelsArray[index]));
    });
  }

  private formatData(array: Array<any>, groupName?: string): any {
    let formattedData = {};
    console.log(groupName);
    formattedData = array?.map((data: any) => ({
      'group': groupName ? groupName : this.title,
      'property': data[0] * 1000,
      'value': Number(this.convertToChartDataUnits(data[1]).replace(/[^\d,.]+/g, ''))
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

  updateChartData(changes?: SimpleChanges): void {
    const changeArr = changes.chartDataArray.currentValue? changes.chartDataArray.currentValue: this.chartDataArray;
    changeArr.forEach((chartData, index) => {
      this.data = this.data.concat(this.formatData(chartData, this.labelsArray[index]));
    });}

  //   this.setChartTicks();
  // }

  private setChartTicks(): string {
    if (!this.chartDataArray || this.chartDataArray.length === 0) {
      this.chartDataUnits = '';
      return this.chartDataUnits;
    }

    let maxValue = 0;
    let maxValueDataUnits = '';

    const allDataValues = this.chartDataArray?.reduce((array: string[], data) => {
      return array.concat(data?.map((values: [number, string]) => values[1]));
    }, []);

    maxValue = allDataValues ? Math.max(...allDataValues.map(Number)) : 0;
    [maxValue, maxValueDataUnits] = this.convertUnits(maxValue).split(' ');

    // const yAxesTicks = this.chart.chart.options.scales.y;
    // yAxesTicks.ticks.callback = (value: any) => {
    //   if (value === 0) {
    //     return null;
    //   }
    //   if (!maxValueDataUnits) {
    //     return `${value}`;
    //   }
    //   return `${value} ${maxValueDataUnits}`;
    // };
    this.chartDataUnits = maxValueDataUnits || '';
    this.options.axes!.left!.ticks!.max = Number(maxValue);
    return this.chartDataUnits;
  }
}
