import { Component, Input, OnChanges } from '@angular/core';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnChanges {
  @Input()
  total: number;
  @Input()
  used: number;
  @Input()
  isBinary = true;
  @Input()
  decimals = 0;

  usedPercentage: number;
  freePercentage: number;

  ngOnChanges() {
    this.usedPercentage = this.total > 0 ? (this.used / this.total) * 100 : 0;
    this.freePercentage = 100 - this.usedPercentage;
  }
}
