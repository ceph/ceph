import { Component, Input, OnChanges, OnInit } from '@angular/core';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnChanges {

  @Input() totalBytes: number;
  @Input() usedBytes: number;

  usedPercentage: number;
  freePercentage: number;
  freeBytes: number;

  constructor() { }

  ngOnChanges() {
    this.usedPercentage = Math.round(this.usedBytes / this.totalBytes * 100);
    this.freePercentage = 100 - this.usedPercentage;
    this.freeBytes = this.totalBytes - this.usedBytes;
  }

}
