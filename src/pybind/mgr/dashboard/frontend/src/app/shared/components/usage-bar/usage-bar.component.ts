import { Component, Input, OnChanges } from '@angular/core';

import _ from 'lodash';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnChanges {
  @Input()
  total: number;
  @Input()
  used: any;
  @Input()
  warningThreshold: number;
  @Input()
  errorThreshold: number;
  @Input()
  isBinary = true;
  @Input()
  decimals = 0;
  @Input()
  calculatePerc = true;

  usedPercentage: number;
  freePercentage: number;

  ngOnChanges() {
    if (this.calculatePerc) {
      this.usedPercentage = this.total > 0 ? (this.used / this.total) * 100 : 0;
      this.freePercentage = 100 - this.usedPercentage;
    } else {
      if (this.used) {
        this.used = this.used.slice(0, -1);
        this.usedPercentage = Number(this.used);
        this.freePercentage = 100 - this.usedPercentage;
      } else {
        this.usedPercentage = 0;
      }
    }
  }
}
