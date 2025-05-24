import { Component, Input, ElementRef, OnInit } from '@angular/core';
import { CssVars } from '../../enum/css-style-variable.enum';
import { CssHelper } from '../../classes/css-helper';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnInit {
  @Input() total: number = 0;
  @Input() used: number = 903741891;
  @Input() warningThreshold?: number = 0.9;
  @Input() errorThreshold?: number = 0.95;
  @Input() isBinary = true;
  data: { group: string; value: number }[] = [{ group: $localize`Capacity`, value: 0 }];
  options;

  constructor(private elementRef: ElementRef, private cssHelper: CssHelper) {
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
          Capacity: this.getCssVariableValue(CssVars.CDS_SUPPORT_INFO)
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
    let color = CssVars.CDS_SUPPORT_INFO; // Default color for 'Capacity'
    const usedPercentage = this.calculateUsedPercentage(this.used, this.total);
    if (this.warningThreshold >= 0 && usedPercentage >= this.warningThreshold) {
      color = CssVars.CDS_SUPPORT_WARNING; // Warning threshold exceeded
    }
    if (this.errorThreshold >= 0 && usedPercentage >= this.errorThreshold) {
      color = CssVars.CDS_SUPPORT_ERROR; // Error threshold exceeded
    }
    this.data[0].value = this.total > 0 ? (this.used / this.total) * 100 : 0;
    this.options.color.scale.Capacity = this.getCssVariableValue(color);
  }

  getCssVariableValue(variableName: string): string {
    const nativeElement = this.elementRef.nativeElement;
    return this.cssHelper.propertyValue(variableName, nativeElement);
  }

  calculateUsedPercentage(used: number, total: number): number {
    return total > 0 ? used / total : 0;
  }
}
