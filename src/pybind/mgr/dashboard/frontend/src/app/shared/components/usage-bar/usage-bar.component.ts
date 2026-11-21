import { ChangeDetectionStrategy, Component, Input, OnChanges, TemplateRef } from '@angular/core';

import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../pipes/dimless.pipe';

export type UsageBarStatus = 'normal' | 'warning' | 'danger';

/**
 * A meter showing how much of a resource is used.
 *
 * It is drawn with plain HTML and CSS and needs no layout measurements, so
 * it is cheap enough to be rendered in every row of a large table.
 */
@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class UsageBarComponent implements OnChanges {
  // Total amount of resource (e.g., disk, memory) available, in bytes.
  @Input({ required: true }) total: number = 0;

  // Amount of resource currently used, in bytes.
  @Input({ required: true }) used: number = 0;

  // Name of the resource, used as the accessible name of the meter.
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

  // Width of the meter (CSS units like `rem`, `px`, `%`). It never grows
  // beyond its container.
  @Input() width: string = '15rem';

  // Text to display when `total` is zero or there’s no data.
  @Input() noDataText: string = '-';

  /**
   * If true, breakdown is shown as a percentage (e.g., "75% used").
   * When false, raw values like "750 MB used" are shown instead.
   */
  @Input() enablePercentageLabel: boolean = true;

  // Optional custom label (e.g., <ng-template><div>Capacity</div></<ng-template>) (overrides default label).
  @Input() customLabel: TemplateRef<any> | string = '';

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

  usedPercent = 0;
  status: UsageBarStatus = 'normal';
  breakdownLabel = '';
  totalLabel = '';
  usedText = '';
  availableText = '';
  totalText = '';

  constructor(
    private dimlessPipe: DimlessPipe,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  get isCustomLabelTemplate(): boolean {
    return this.customLabel instanceof TemplateRef;
  }

  ngOnChanges(): void {
    const ratio = this.total > 0 ? this.used / this.total : 0;
    this.usedPercent = Math.min(Math.max(ratio * 100, 0), 100);
    this.status = this.getStatus(ratio);
    this.breakdownLabel = this.getBreakdownFormatter();
    this.totalLabel = this.getTotalFormatter();
    this.usedText = this.formatValue(this.used);
    this.availableText = this.formatValue(this.total - this.used);
    this.totalText = this.formatValue(this.total);
  }

  private getStatus(ratio: number): UsageBarStatus {
    // Thresholds are only applied when both are known (e.g. OSD settings may
    // still be loading, or report -1 on error).
    if (!(this.warningThreshold > 0 && this.errorThreshold > 0)) {
      return 'normal';
    }
    if (ratio >= this.errorThreshold) {
      return 'danger';
    }
    if (ratio >= this.warningThreshold) {
      return 'warning';
    }
    return 'normal';
  }

  private formatValue(value: number): string {
    return this.isBinary
      ? this.dimlessBinaryPipe.transform(value, this.decimals)
      : this.dimlessPipe.transform(value, this.decimals);
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
