import { Component, Input, ElementRef, OnInit } from '@angular/core';
import { CssHelper } from '../../classes/css-helper';
import { ChartTabularData, MeterChartOptions, Statuses } from '@carbon/charts-angular';
import { MeterChartColor } from '../../enum/css-style-variable.enum';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../pipes/dimless.pipe';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnInit {
  @Input() total: number;
  @Input() used: number;
  @Input() warningThreshold?: number = 0.9;
  @Input() errorThreshold?: number = 0.95;
  @Input() isBinary = true;
  @Input() decimal: number = 2;
  data: ChartTabularData = [{ group: $localize`Capacity`, value: 0 }];
  options: MeterChartOptions = {
    legend: {
      enabled: false
    },
    meter: {
      status: {
        ranges: [
          {
            range: [0, 60],
            status: Statuses.SUCCESS
          },
          {
            range: [60, 80],
            status: Statuses.WARNING
          },
          {
            range: [80, 100],
            status: Statuses.DANGER
          }
        ]
      }
    },
    tooltip: {
      enabled: true
    },
    toolbar: {
      enabled: false
    },
    height: '2rem',
    width: '15rem'
  };

  constructor(
    private elementRef: ElementRef,
    private cssHelper: CssHelper,
    private dimlessPipe: DimlessPipe,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  ngOnInit() {
    const usedPercentage = this.calculateUsed(this.used, this.total);
    this.data[0].value = usedPercentage;

    this.options = {
      ...this.options,
      color: {
        scale: {
          Capacity: this.getCssVariableValue(MeterChartColor.CDS_SUPPORT_INFO)
        }
      },
      meter: {
        ...this.options.meter,
        proportional: {
          total: this.total,
          totalFormatter: (total: number) =>
            this.isBinary
              ? $localize`${this.dimlessBinaryPipe.transform(total, this.decimal)} total`
              : $localize`${this.dimlessPipe.transform(total, this.decimal)} total`,
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
      }
    };
  }

  getCssVariableValue(variableName: string): string {
    const nativeElement = this.elementRef.nativeElement;
    return this.cssHelper.propertyValue(variableName, nativeElement);
  }

  calculateUsed(used: number, total: number): number {
    return total > 0 ? used / total : 0;
  }
}
