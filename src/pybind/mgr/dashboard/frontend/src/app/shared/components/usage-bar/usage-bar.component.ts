import { Component, Input, ElementRef, OnInit } from '@angular/core';
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
  @Input({ required: true }) total: number = 0; // Bytes
  @Input({ required: true }) used: number = 0; // Bytes
  @Input() title: string = '';
  @Input() warningThreshold: number = 0.9;
  @Input() errorThreshold: number = 0.95;
  @Input() isBinary: boolean = true;
  @Input() decimal: number = 2;
  @Input() proportional: boolean = false;
  @Input() height: string = '2rem';
  @Input() width: string = '15rem';

  data: ChartTabularData = [];
  options: MeterChartOptions = {
    legend: {
      enabled: false
    },
    meter: {
      status: {
        ranges: []
      }
    },
    tooltip: {
      enabled: true
    },
    toolbar: {
      enabled: false
    },
    height: this.height,
    width: this.width,
    animations: false
  };

  constructor(
    private elementRef: ElementRef,
    private cssHelper: CssHelper,
    private dimlessPipe: DimlessPipe,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  ngOnInit(): void {
    const usedCapacity = this.total > 0 ? this.used / this.total : 0;
    if (this.proportional) {
      this.data = [
        {
          group: this.title,
          value: this.used
        }
      ];
    } else {
      this.data = [
        {
          group: this.title,
          value: usedCapacity * 100
        }
      ];
    }

    const thresholds: MeterChartOptions['meter']['status']['ranges'] = this.proportional
      ? [
          {
            range: [0, this.total * this.warningThreshold],
            status: Statuses.SUCCESS
          },
          {
            range: [this.total * this.warningThreshold, this.total * this.errorThreshold],
            status: Statuses.WARNING
          },
          {
            range: [this.total * this.errorThreshold, this.total],
            status: Statuses.DANGER
          }
        ]
      : [
          {
            range: [0, this.warningThreshold * 100],
            status: Statuses.SUCCESS
          },
          {
            range: [this.warningThreshold * 100, this.errorThreshold * 100],
            status: Statuses.WARNING
          },
          {
            range: [this.errorThreshold * 100, 100],
            status: Statuses.DANGER
          }
        ];

    const meterBase = {
      status: { ranges: thresholds },
      ...(this.proportional && {
        proportional: {
          total: this.total,
          unit: 'B', // Bytes
          totalFormatter: () =>
            this.isBinary
              ? $localize`${this.dimlessBinaryPipe.transform(this.total, this.decimal)} total`
              : $localize`${this.dimlessPipe.transform(this.total, this.decimal)} total`,
          breakdownFormatter: () => {
            const used = this.isBinary
              ? this.dimlessBinaryPipe.transform(this.used, this.decimal)
              : this.dimlessPipe.transform(this.used, this.decimal);
            const available = this.isBinary
              ? this.dimlessBinaryPipe.transform(this.total - this.used, this.decimal)
              : this.dimlessPipe.transform(this.total - this.used, this.decimal);
            return $localize`${used} used (${available} available)`;
          }
        }
      })
    };

    this.options = {
      ...this.options,
      ...(this.proportional && {
        color: {
          scale: {
            [this.title]: this.getStatusFromThresholds(this.data[0].value, thresholds)
          }
        }
      }),
      meter: {
        ...meterBase
      }
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
}
