import { Component, Input, OnChanges } from '@angular/core';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnChanges {
  @Input()
  totalBytes: number;
  @Input()
  usedBytes: number;
  @Input()
  decimals = 0;

  usedPercentage: number;
  freePercentage: number;
  freeBytes: number;

  constructor() {}

  ngOnChanges() {
    this.usedPercentage = this.totalBytes > 0 ? (this.usedBytes / this.totalBytes) * 100 : 0;
    this.freePercentage = 100 - this.usedPercentage;
    this.freeBytes = this.totalBytes - this.usedBytes;
  }
}
