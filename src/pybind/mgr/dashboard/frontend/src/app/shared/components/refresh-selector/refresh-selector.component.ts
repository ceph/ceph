import { Component, OnInit } from '@angular/core';

import { RefreshIntervalService } from '../../services/refresh-interval.service';

@Component({
  selector: 'cd-refresh-selector',
  templateUrl: './refresh-selector.component.html',
  styleUrls: ['./refresh-selector.component.scss']
})
export class RefreshSelectorComponent implements OnInit {
  selectedInterval: number;
  intervalList: { [key: string]: number } = {
    '5 s': 5000,
    '10 s': 10000,
    '15 s': 15000,
    '30 s': 30000,
    '1 min': 60000,
    '3 min': 180000,
    '5 min': 300000
  };
  intervalKeys = Object.keys(this.intervalList);

  constructor(private refreshIntervalService: RefreshIntervalService) {}

  ngOnInit() {
    this.selectedInterval = this.refreshIntervalService.getRefreshInterval() || 5000;
  }

  changeRefreshInterval(interval: number) {
    this.refreshIntervalService.setRefreshInterval(interval);
  }
}
