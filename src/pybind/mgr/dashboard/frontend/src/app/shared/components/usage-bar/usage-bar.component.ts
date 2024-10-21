import { Component, Input, ElementRef, OnInit, TemplateRef } from '@angular/core';
import { ChartTabularData, MeterChartOptions, Statuses } from '@carbon/charts-angular';
import { StatusToCssMap } from '../../enum/usage-bar-chart.enum';
import { CssHelper } from '../../classes/css-helper';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../pipes/dimless.pipe';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnInit {
  @Input({ required: true }) total: number = 0; // in bytes
  @Input({ required: true }) used: number = 0; // in bytes
  @Input() title: string = '';
  @Input() warningThreshold: number = 0.9;
  @Input() errorThreshold: number = 0.95;
  @Input() isBinary: boolean = true;
  @Input() decimals: number = 2;
  @Input() proportional: boolean = true;

  @Input() height: string = '2rem';
  @Input() width: string = '15rem';
  @Input() totalUnit: string = 'B'; // Bytes
  @Input() animations: boolean = false;

  @Input() legendEnabled: boolean = false;
  @Input() toolbarEnabled: boolean = false;
  @Input() noDataText: string = '-';

  @Input() tooltipOptions?: MeterChartOptions['tooltip'] = {};
  @Input() customLabel: TemplateRef<any> | string = '';
  @Input() customColorScale?: { [key: string]: string };
  @Input() customTotalFormatter?: (total: number) => string;
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
    const usedCapacity = this.total > 0 ? this.used / this.total : 0;

    let thresholds: MeterChartOptions['meter']['status']['ranges'] = [];
    let chartData: ChartTabularData = [];
    let colorScale: { [key: string]: string } | undefined;
    let proportionalConfig: MeterChartOptions['meter']['proportional'] | undefined;
    let meterBase: MeterChartOptions['meter'] = {};

    let chartOptions: MeterChartOptions = {
      legend: { enabled: this.legendEnabled },
      toolbar: { enabled: this.toolbarEnabled },
      tooltip: {
        enabled: true,
        ...this.tooltipOptions,
        customHTML: this.tooltipOptions?.customHTML || (() => this.defaultTooltip())
      },
      animations: this.animations,
      height: this.height,
      width: this.width
    };

    if (this.proportional) {
      chartData = [{ group: this.title, value: this.used }];
      thresholds = [
        { range: [0, this.total * this.warningThreshold], status: Statuses.SUCCESS },
        {
          range: [this.total * this.warningThreshold, this.total * this.errorThreshold],
          status: Statuses.WARNING
        },
        { range: [this.total * this.errorThreshold, this.total], status: Statuses.DANGER }
      ];

      proportionalConfig = {
        total: this.total,
        unit: this.totalUnit,
        totalFormatter: () => this.getTotalFormatter(),
        breakdownFormatter: () => this.getBreakdownFormatter()
      };

      colorScale = this.customColorScale || {
        [this.title]: this.getStatusFromThresholds(this.used, thresholds)
      };
      meterBase = {
        status: { ranges: thresholds },
        showLabels: !this.customLabel,
        proportional: proportionalConfig
      };
      chartOptions = {
        ...chartOptions,
        color: { scale: colorScale },
        meter: meterBase
      };
    } else {
      chartData = [{ group: '', value: usedCapacity * 100 }];
      thresholds = [
        { range: [0, this.warningThreshold * 100], status: Statuses.SUCCESS },
        {
          range: [this.warningThreshold * 100, this.errorThreshold * 100],
          status: Statuses.WARNING
        },
        { range: [this.errorThreshold * 100, 100], status: Statuses.DANGER }
      ];
      meterBase = {
        status: { ranges: thresholds },
        showLabels: !this.customLabel
      };
      chartOptions = {
        ...chartOptions,
        meter: meterBase
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
    return this.getCssVariableValue(
      StatusToCssMap[Statuses.SUCCESS as keyof typeof StatusToCssMap]
    );
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
    return $localize`${used} used (${available} available) &nbsp;&nbsp; ${total} total`;
  }

  private getTotalFormatter(): string {
    return this.customTotalFormatter
      ? this.customTotalFormatter!(this.total)
      : $localize`${this.formatValue(this.total)} total`;
  }

  private getBreakdownFormatter(): string {
    return this.customBreakdownFormatter
      ? this.customBreakdownFormatter!({ used: this.used, total: this.total })
      : $localize`${this.formatValue(this.used)} used`;
  }
}
