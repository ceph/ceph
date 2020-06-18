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

  usedPercentage: number;
  freePercentage: number;
  freeBytes: number;

  ngOnChanges() {
    this.usedPercentage = Math.round((this.usedBytes / this.totalBytes) * 100);
    this.freePercentage = 100 - this.usedPercentage;
    this.freeBytes = this.totalBytes - this.usedBytes;
  }
}
