import { Component, Input, ElementRef, OnInit, TemplateRef } from '@angular/core';
import { ChartTabularData, MeterChartOptions, Statuses } from '@carbon/charts-angular';
import { StatusToCssMap } from '../../enum/usage-bar-chart.enum';
import { CssHelper } from '../../classes/css-helper';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../pipes/dimless.pipe';

const PRIMARY_COLOR = 'primary'; // Default theme primary color variable

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnInit {
  // Total amount of resource (e.g., disk, memory) available, in bytes.
  @Input({ required: true }) total: number = 0;

  // Amount of resource currently used, in bytes.
  @Input({ required: true }) used: number = 0;

  // Optional label or title for the chart, used in legends and tooltips.
  @Input() title: string = '';

  // Threshold (0–1) to mark the start of the "warning" zone (e.g., 0.5 = 50%).
  @Input() warningThreshold: number;

  // Threshold (0–1) to mark the start of the "danger" zone (e.g., 0.8 = 80%).
  @Input() errorThreshold: number;

  /**
   * If true, values are formatted using binary units (e.g., KiB, MiB).
   * If false, decimal units are used (e.g., kB, MB).
   */
  @Input() isBinary: boolean = true;

  // Number of decimal places to show in formatted numbers.
  @Input() decimals: number = 2;

  // Height of the chart component (CSS units like `rem`, `px`, `%`).
  @Input() height: string = '2rem';

  // Width of the chart component (CSS units like `rem`, `px`, `%`).
  @Input() width: string = '15rem';

  // Custom unit label for the total value (e.g., "B", "kB", "requests").
  @Input() totalUnit: string = 'B';

  // Whether chart animations are enabled (e.g., grow bars on change).
  @Input() animations: boolean = false;

  // Enables the legend section (useful for grouped charts).
  @Input() legendEnabled: boolean = false;

  // Enables the toolbar (for exporting, toggling datasets, etc., depending on chart lib).
  @Input() toolbarEnabled: boolean = false;

  // Text to display when `total` is zero or there’s no data.
  @Input() noDataText: string = '-';

  /**
   * If true, breakdown is shown as a percentage (e.g., "75% used").
   * When false, raw values like "750 MB used" are shown instead.
   */
  @Input() enablePercentageLabel: boolean = true;

  // Optional custom label (e.g., <ng-template><div>Capacity</div></<ng-template>) (overrides default label).
  @Input() customLabel: TemplateRef<any> | string = '';

  // Custom tooltip options passed directly to the chart library.
  @Input() tooltipOptions?: MeterChartOptions['tooltip'] = {};

  // Optional override for the default color scale (e.g., { Memory: "#ff0000" }).
  @Input() customColorScale?: { [key: string]: string };

  /**
   * Optional custom formatter for the total value (overrides default formatter).
   * Example usage: (total) => `${total} requests`
   */
  @Input() customTotalFormatter?: (total: number) => string;

  /**
   * Optional custom formatter for the breakdown section (overrides default formatter).
   * Example usage: ({ used, total }) => `${used} of ${total} used`
   */
  @Input() customBreakdownFormatter?: (x: { used: number; total: number }) => string;

  data: ChartTabularData = [];
  options: MeterChartOptions = {};

  constructor(
    private elementRef: ElementRef,
    private cssHelper: CssHelper,
    private dimlessPipe: DimlessPipe,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  get isCustomLabelTemplate(): boolean {
    return this.customLabel instanceof TemplateRef;
  }

  ngOnInit(): void {
    const chartData: ChartTabularData = [{ group: this.title || '', value: this.used }];
    let thresholds: MeterChartOptions['meter']['status']['ranges'] = [];
    if (this.warningThreshold && this.errorThreshold) {
      thresholds = [
        {
          range: [this.total * this.warningThreshold, this.total * this.errorThreshold],
          status: Statuses.WARNING
        },
        { range: [this.total * this.errorThreshold, this.total], status: Statuses.DANGER }
      ];
    }
    const colorScale = this.customColorScale || {
      [this.title || '']: this.getStatusFromThresholds(this.used, thresholds)
    };

    const chartOptions: MeterChartOptions = {
      legend: { enabled: this.legendEnabled },
      toolbar: { enabled: this.toolbarEnabled },
      tooltip: {
        enabled: true,
        ...this.tooltipOptions,
        customHTML: this.tooltipOptions?.customHTML || (() => this.defaultTooltip())
      },
      animations: this.animations,
      height: this.height,
      width: this.width,
      color: { scale: colorScale },
      meter: {
        showLabels: !this.customLabel,
        proportional: {
          total: this.total,
          unit: this.totalUnit,
          totalFormatter: () => this.getTotalFormatter(),
          breakdownFormatter: () => this.getBreakdownFormatter()
        }
      }
    };
    if (thresholds.length > 0) {
      chartOptions.meter = {
        ...chartOptions.meter,
        status: {
          ranges: thresholds
        }
      };
    }
    this.data = chartData;
    this.options = chartOptions;
  }

  private getStatusFromThresholds(
    value: number,
    thresholds: MeterChartOptions['meter']['status']['ranges']
  ): string {
    for (const threshold of thresholds) {
      const [min, max] = threshold.range;
      if (value >= min && value < max) {
        return this.getCssVariableValue(
          StatusToCssMap[threshold.status as keyof typeof StatusToCssMap]
        );
      }
    }
    return this.getCssVariableValue(PRIMARY_COLOR);
  }

  private getCssVariableValue(variableName: string): string {
    return this.cssHelper.propertyValue(variableName, this.elementRef.nativeElement);
  }

  private formatValue(value: number): string {
    return this.isBinary
      ? this.dimlessBinaryPipe.transform(value, this.decimals)
      : this.dimlessPipe.transform(value, this.decimals);
  }

  private defaultTooltip(): string {
    const used = this.formatValue(this.used);
    const available = this.formatValue(this.total - this.used);
    const total = this.formatValue(this.total);
    return `<div class="meter-tooltip">
      <div><strong>Used:</strong> ${used}</div>
      <div><strong>Available:</strong> ${available}</div>
      <div><strong>Total:</strong> ${total}</div>
    </div>`;
  }

  private getTotalFormatter(): string {
    if (this.customTotalFormatter) {
      return this.customTotalFormatter!(this.total);
    }
    if (this.enablePercentageLabel && this.total > 0) {
      return '';
    }

    return $localize`${this.formatValue(this.total)} total`;
  }

  private getBreakdownFormatter(): string {
    if (this.customBreakdownFormatter) {
      return this.customBreakdownFormatter!({ used: this.used, total: this.total });
    }

    if (this.enablePercentageLabel && this.total > 0) {
      const percentage = (this.used / this.total) * 100;
      if (percentage < 0.01) {
        return '0%';
      }

      const formatted = percentage.toFixed(this.decimals);
      return `${formatted}%`;
    }

    return $localize`${this.formatValue(this.used)} used`;
  }
}
