import {
  ChangeDetectorRef,
  Component,
  EventEmitter,
  inject,
  Input,
  OnChanges,
  Output,
  SimpleChanges
} from '@angular/core';
import {
  AreaChartOptions,
  ChartTabularData,
  ToolbarControlTypes,
  ScaleTypes,
  ChartsModule
} from '@carbon/charts-angular';
import merge from 'lodash.merge';
import { NumberFormatterService } from '../../services/number-formatter.service';
import { DatePipe } from '@angular/common';
import { ChartPoint } from '../../models/area-chart-point';
import {
  DECIMAL,
  formatValues,
  getDisplayUnit,
  getDivisor,
  getLabels
} from '../../helpers/unit-format-utils';

@Component({
  selector: 'cd-area-chart',
  standalone: true,
  templateUrl: './area-chart.component.html',
  styleUrl: './area-chart.component.scss',
  imports: [ChartsModule]
})
export class AreaChartComponent implements OnChanges {
  @Input() chartType = 'line';
  @Input() chartTitle = '';
  @Input() dataUnit = '';
  @Input() rawData!: ChartPoint[];
  @Input() chartKey = '';
  @Input() decimals = DECIMAL;
  @Input() customOptions?: Partial<AreaChartOptions>;
  @Input() legendEnabled = true;
  @Input() subHeading = '';
  @Input() height = '300px';

  @Output() currentFormattedValues = new EventEmitter<{
    key: string;
    values: Record<string, string>;
  }>();

  chartData: ChartTabularData = [];
  chartOptions!: AreaChartOptions;

  private chartDisplayUnit = '';

  private cdr = inject(ChangeDetectorRef);
  private numberFormatter: NumberFormatterService = inject(NumberFormatterService);
  private datePipe = inject(DatePipe);
  private lastEmittedRawValues?: Record<string, number>;

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['rawData'] && this.rawData?.length) {
      this.updateChart();
    }
  }

  // Convert raw data structure to tabular format accepted by Carbon charts.
  private transformToChartData(data: ChartPoint[]): ChartTabularData {
    return data.flatMap(({ timestamp, values }) =>
      Object.entries(values).map(([group, value]) => ({
        group,
        date: timestamp,
        value
      }))
    );
  }

  // Main method to process chart input, compute scale, format labels,
  // and merge with custom chart options if provided.
  private updateChart(): void {
    this.chartData = this.transformToChartData(this.rawData);

    const max = Math.max(...this.chartData.map((d) => d['value']), 1);

    const labels = getLabels(this.dataUnit, this.numberFormatter);
    const divisor = getDivisor(this.dataUnit);
    this.chartDisplayUnit = getDisplayUnit(max, this.dataUnit, labels, divisor);

    this.emitCurrentFomattedValue();

    // Merge base and custom chart options
    const defaultOptions = this.getChartOptions(max, labels, divisor);
    this.chartOptions = merge({}, defaultOptions, this.customOptions || {});
    this.cdr.detectChanges();
  }

  private emitCurrentFomattedValue() {
    const latestEntry = this.rawData[this.rawData.length - 1];
    if (!latestEntry) return;

    if (
      this.lastEmittedRawValues &&
      Object.keys(latestEntry.values).every(
        (value) => latestEntry.values[value] === this.lastEmittedRawValues?.[value]
      )
    ) {
      return;
    }

    const formattedValues: Record<string, string> = {};
    for (const [group, value] of Object.entries(latestEntry.values)) {
      formattedValues[group] = formatValues(
        value ?? 0,
        this.dataUnit,
        this.numberFormatter,
        this.decimals
      );
    }

    this.currentFormattedValues.emit({
      key: this.chartKey,
      values: formattedValues
    });

    this.lastEmittedRawValues = { ...latestEntry.values };
  }

  private getChartOptions(max: number, labels: string[], divisor: number): AreaChartOptions {
    return {
      legend: {
        enabled: this.legendEnabled
      },
      axes: {
        bottom: {
          mapsTo: 'date',
          scaleType: ScaleTypes.TIME,
          ticks: {
            number: 4,
            rotateIfSmallerThan: 0
          }
        },
        left: {
          title: `${this.chartTitle}${this.chartDisplayUnit ? ` (${this.chartDisplayUnit})` : ''}`,
          mapsTo: 'value',
          scaleType: ScaleTypes.LINEAR,
          domain: [0, max],
          ticks: {
            // Only return numeric part of the formatted string (exclude units)
            formatter: (tick: number | Date): string => {
              const raw = this.formatValueForChart(tick, labels, divisor);
              const num = parseFloat(raw);
              return num.toString();
            }
          }
        }
      },
      tooltip: {
        enabled: true,
        showTotal: false,
        valueFormatter: (value: number): string =>
          (this.formatValueForChart(value, labels, divisor) || value).toString(),
        customHTML: (data, defaultHTML) => this.formatChartTooltip(defaultHTML, data)
      },
      points: {
        enabled: false
      },
      toolbar: {
        enabled: false,
        controls: [
          {
            type: ToolbarControlTypes.EXPORT_CSV
          },
          {
            type: ToolbarControlTypes.EXPORT_PNG
          },
          {
            type: ToolbarControlTypes.EXPORT_JPG
          },
          {
            type: ToolbarControlTypes.SHOW_AS_DATATABLE
          }
        ]
      },
      animations: false,
      height: this.height,
      data: {
        loading: !this.chartData?.length
      }
    };
  }

  // Custom tooltip formatter to replace default timestamp with a formatted one.
  formatChartTooltip(defaultHTML: string, data: { date: Date }[]): string {
    if (!data?.length) return defaultHTML;

    const formattedTime = this.datePipe.transform(data[0].date, 'dd MMM, HH:mm:ss');
    return defaultHTML.replace(
      /<p class="value">.*?<\/p>/,
      `<p class="value">${formattedTime}</p>`
    );
  }

  // Uses number formatter service to convert chart value based on unit and divisor.
  private formatValueForChart(input: number | Date, labels: string[], divisor: number): string {
    if (typeof input !== 'number') return '';
    return this.numberFormatter.formatFromTo(
      input,
      this.dataUnit,
      this.chartDisplayUnit,
      divisor,
      labels,
      this.decimals
    );
  }
}
