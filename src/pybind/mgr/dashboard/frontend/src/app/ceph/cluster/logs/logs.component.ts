import { Component, OnDestroy, OnInit } from '@angular/core';

import { DashboardService } from '../../../shared/api/dashboard.service';

@Component({
  selector: 'cd-logs',
  templateUrl: './logs.component.html',
  styleUrls: ['./logs.component.scss']
})
export class LogsComponent implements OnInit, OnDestroy {
  contentData: any;
  interval: number;

  constructor(private dashboardService: DashboardService) {}

  ngOnInit() {
    this.getInfo();
    this.interval = window.setInterval(() => {
      this.getInfo();
    }, 5000);
  }

  ngOnDestroy() {
    clearInterval(this.interval);
  }

  getInfo() {
    this.dashboardService.getHealth().subscribe((data: any) => {
      this.contentData = data;
    });
  }
}
