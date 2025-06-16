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
  @Input() unit: string = 'B'; // Bytes
  @Input() animations: boolean = false;

  @Input() legendEnabled: boolean = false;
  @Input() toolbarEnabled: boolean = false;
  @Input() noDataText: string = '-';

  @Input() tooltipOptions?: MeterChartOptions['tooltip'] = {};
  @Input() customLabel: TemplateRef<any> | string = '';
  @Input() customColorScale?: { [key: string]: string };
  @Input() totalFormatter?: (total: number) => string;
  @Input() breakdownFormatter?: (x: { used: number; total: number }) => string;

  data: ChartTabularData = [];
  options: MeterChartOptions = {
    legend: { enabled: this.legendEnabled },
    tooltip: { enabled: true, ...this.tooltipOptions },
    toolbar: { enabled: this.toolbarEnabled },
    animations: this.animations,
    height: this.height,
    width: this.width,
    meter: {
      status: { ranges: [] }
    }
  };

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

    this.data = this.proportional
      ? [{ group: this.title, value: this.used }]
      : [{ group: '', value: usedCapacity * 100 }];

    const thresholds: MeterChartOptions['meter']['status']['ranges'] = this.proportional
      ? [
          { range: [0, this.total * this.warningThreshold], status: Statuses.SUCCESS },
          {
            range: [this.total * this.warningThreshold, this.total * this.errorThreshold],
            status: Statuses.WARNING
          },
          { range: [this.total * this.errorThreshold, this.total], status: Statuses.DANGER }
        ]
      : [
          { range: [0, this.warningThreshold * 100], status: Statuses.SUCCESS },
          {
            range: [this.warningThreshold * 100, this.errorThreshold * 100],
            status: Statuses.WARNING
          },
          { range: [this.errorThreshold * 100, 100], status: Statuses.DANGER }
        ];

    const defaultTooltipOptions: MeterChartOptions['tooltip'] = {
      enabled: true,
      showTotal: false,
      groupLabel: 'Usage',
      valueFormatter: (value: any) =>
        this.isBinary
          ? `${this.dimlessBinaryPipe.transform(value, this.decimals)}`
          : `${this.dimlessPipe.transform(value, this.decimals)}`,
      customHTML: () => {
        const used = this.formatValue(this.used);
        const available = this.formatValue(this.total - this.used);
        const total = this.formatValue(this.total);
        return $localize`
                ${used} used (${available} available) &nbsp;&nbsp; ${total} total
            `;
      }
    };
    const meterBase: MeterChartOptions['meter'] = {
      status: { ranges: thresholds },
      showLabels: !this.customLabel,
      ...(this.proportional && {
        proportional: {
          total: this.total,
          unit: this.unit,
          totalFormatter: () =>
            this.totalFormatter
              ? this.totalFormatter(this.total)
              : $localize`${this.formatValue(this.total)} total`,
          breakdownFormatter: () => {
            return this.breakdownFormatter
              ? this.breakdownFormatter({ used: this.used, total: this.total })
              : $localize`${this.formatValue(this.used)} used`;
          }
        }
      })
    };

    this.options = {
      legend: { enabled: this.legendEnabled },
      toolbar: { enabled: this.toolbarEnabled },
      tooltip: {
        ...defaultTooltipOptions,
        ...this.tooltipOptions,
        customHTML: this.tooltipOptions?.customHTML || defaultTooltipOptions.customHTML
      },
      animations: this.animations,
      height: this.height,
      width: this.width,
      ...(this.proportional && {
        color: {
          scale: this.customColorScale || {
            [this.title]: this.getStatusFromThresholds(this.data[0].value, thresholds)
          }
        }
      }),
      meter: meterBase
    };
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
}
