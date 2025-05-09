import { Component, Input, ElementRef, OnInit } from '@angular/core';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnInit {
  @Input() total: number = 0;
  @Input() used: number = 0;
  @Input() warningThreshold?: number = 0.9;
  @Input() errorThreshold?: number = 0.95;
  @Input() isBinary = true;
  @Input() decimals: number = 0;
  @Input() title = $localize`usage`;
  data: { group: string; value: number }[] = [{ group: $localize`Capacity`, value: 0 }];
  options;

  constructor(private elementRef: ElementRef) {
    this.options = {
      resizable: false,
      meter: {
        showLabels: false
      },
      tooltip: {
        enabled: true
      },
      color: {
        scale: {
          Capacity: this.getCssVariableValue('cds-support-info')
        }
      },
      height: '1rem',
      width: '12rem',
      toolbar: {
        enabled: false
      }
    };
  }

  ngOnInit() {
    let color = 'cds-support-info'; // Default color for 'Capacity'
    const usedPercentage = this.calculateUsedPercentage(this.used, this.total);
    if (this.warningThreshold >= 0 && usedPercentage >= this.warningThreshold) {
      color = 'cds-support-warning'; // Warning threshold exceeded
    }
    if (this.errorThreshold >= 0 && usedPercentage >= this.errorThreshold) {
      color = 'cds-support-error'; // Error threshold exceeded
    }
    this.data[0].value = this.total > 0 ? (this.used / this.total) * 100 : 0;
    this.options.color.scale.Capacity = this.getCssVariableValue(color);
  }

  getCssVariableValue(variableName: string): string {
    const nativeElement = this.elementRef.nativeElement;
    return getComputedStyle(nativeElement).getPropertyValue(`--${variableName}`);
  }

  calculateUsedPercentage(used: number, total: number): number {
    return total > 0 ? used / total : 0;
  }
}
