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
import {
  computeTimeIntervalName,
  formatTick,
  options as carbonChartPresets,
  timeScale as carbonTimeScaleDefaults,
  TimeIntervalFormats
} from '@carbon/charts';
import merge from 'lodash.merge';
import { NumberFormatterService } from '../../services/number-formatter.service';
import { ChartPoint } from '../../models/area-chart-point';
import {
  DECIMAL,
  formatValues,
  getDisplayUnit,
  getDivisor,
  getLabels
} from '../../helpers/unit-format-utils';

const DEFAULT_TICKS_COUNT = 4;

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
    this.chartOptions = merge(
      {},
      { timeScale: merge({}, carbonTimeScaleDefaults) },
      defaultOptions,
      this.customOptions || {}
    );
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
    // Check if time span is approximately 5 minutes (300 seconds ± 30 seconds tolerance)
    const timeSpan = this.getTimeSpanInSeconds();
    const isFiveMinuteSpan = timeSpan > 0 && timeSpan >= 270 && timeSpan <= 330;

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
        valueFormatter: (value: number | Date): string => {
          if (value instanceof Date) {
            return value.toString();
          }
          return (
            this.formatValueForChart(value, labels, divisor, this.decimals) || value
          ).toString();
        },
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

  // Tooltip: use "Time" instead of Carbon's default "x-value", and format the
  // timestamp with the same rules as the chart's time axis (formatTick).
  formatChartTooltip(defaultHTML: string, data: { date: Date }[]): string {
    if (!data?.length) return defaultHTML;

    const timeMs = new Date(data[0].date).getTime();
    const tickCount = this.chartOptions?.axes?.bottom?.ticks?.number ?? DEFAULT_TICKS_COUNT;
    const tickStops = this.getTimeAxisTickStops(tickCount);
    const timeScaleOpts = merge({}, carbonTimeScaleDefaults, this.chartOptions?.timeScale ?? {});
    const interval = computeTimeIntervalName(
      tickStops,
      timeScaleOpts.timeInterval as keyof TimeIntervalFormats | undefined
    );
    const tickIndex = tickStops.reduce(
      (bestI, t, i, arr) => (Math.abs(t - timeMs) < Math.abs(arr[bestI] - timeMs) ? i : bestI),
      0
    );
    const locale = merge({}, carbonChartPresets.areaChart.locale, this.chartOptions?.locale ?? {});
    const formattedTime = formatTick(timeMs, tickIndex, tickStops, interval, timeScaleOpts, locale);

    return defaultHTML
      .replace(/<p>x-value<\/p>/i, '<p>Time</p>')
      .replace(/<p class="value">.*?<\/p>/, `<p class="value">${formattedTime}</p>`);
  }

  /** Approximate time-axis tick positions (same count as axis) for timeScale formatting. */
  private getTimeAxisTickStops(tickCount: number): number[] {
    if (!this.rawData?.length) return [];
    const times = this.rawData.map((p) => new Date(p.timestamp).getTime());
    const min = Math.min(...times);
    const max = Math.max(...times);
    if (max === min) {
      return [min];
    }
    const tickCtn = Math.max(2, tickCount);
    return Array.from({ length: tickCtn }, (_, i) => min + ((max - min) * i) / (tickCtn - 1));
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
