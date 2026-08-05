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
  ChartsModule,
  TickRotations
} from '@carbon/charts-angular';
import merge from 'lodash/merge';
import { NumberFormatterService } from '../../services/number-formatter.service';
import { ChartPoint } from '../../models/area-chart-point';
import {
  DECIMAL,
  formatValues,
  getDisplayUnit,
  getDivisor,
  getLabels
} from '../../helpers/unit-format-utils';
import { DatePipe } from '@angular/common';

const DEFAULT_TICKS_COUNT = 4;
const FIVE_MINUTE_SPAN_SECONDS = 300;
const TIME_SPAN_TOLERANCE_SECONDS = 30;
const TOOLTIP_TIME_FORMAT = 'MMM d, hh:mm a';

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
  @Input() axisDecimals?: number = 1;
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
  private lastEmittedRawValues?: Record<string, number>;

  private datePipe = inject(DatePipe);

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

  // Calculate time span in seconds from raw data timestamps
  private getTimeSpanInSeconds(): number {
    if (!this.rawData || this.rawData.length < 2) {
      return 0;
    }
    const firstTimestamp = new Date(this.rawData[0].timestamp).getTime();
    const lastTimestamp = new Date(this.rawData[this.rawData.length - 1].timestamp).getTime();
    return (lastTimestamp - firstTimestamp) / 1000;
  }

  private getChartOptions(max: number, labels: string[], divisor: number): AreaChartOptions {
    const timeSpan = this.getTimeSpanInSeconds();
    const isFiveMinuteSpan =
      timeSpan > 0 &&
      timeSpan >= FIVE_MINUTE_SPAN_SECONDS - TIME_SPAN_TOLERANCE_SECONDS &&
      timeSpan <= FIVE_MINUTE_SPAN_SECONDS + TIME_SPAN_TOLERANCE_SECONDS;
    return {
      legend: {
        enabled: this.legendEnabled
      },
      axes: {
        bottom: {
          mapsTo: 'date',
          scaleType: ScaleTypes.TIME,
          ticks: {
            number: DEFAULT_TICKS_COUNT,
            rotation: isFiveMinuteSpan ? TickRotations.ALWAYS : TickRotations.AUTO
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
              const raw = this.formatValueForChart(tick, labels, divisor, this.axisDecimals);
              const num = parseFloat(raw);
              return Number.isNaN(num) ? '' : num.toString();
            }
          }
        }
      },
      tooltip: {
        enabled: true,
        showTotal: false,
        truncation: {
          type: 'none'
        },
        valueFormatter: (value: number | Date, label: string): string => {
          if (value instanceof Date && label === 'x-value') {
            return this.datePipe.transform(value, TOOLTIP_TIME_FORMAT) ?? '';
          }
          return (
            this.formatValueForChart(value, labels, divisor, this.decimals) || value
          ).toString();
        },
        customHTML: (_data, defaultHTML) => this.formatChartTooltip(defaultHTML)
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

  formatChartTooltip(defaultHTML: string): string {
    return defaultHTML.replace(/<p>x-value<\/p>/i, `<p>${$localize`Time`}</p>`);
  }

  // Uses number formatter service to convert chart value based on unit and divisor.
  private formatValueForChart(
    input: number | Date,
    labels: string[],
    divisor: number,
    decimals: number
  ): string {
    if (typeof input !== 'number') return '';
    return this.numberFormatter.formatFromTo(
      input,
      this.dataUnit,
      this.chartDisplayUnit,
      divisor,
      labels,
      decimals
    );
  }
}
