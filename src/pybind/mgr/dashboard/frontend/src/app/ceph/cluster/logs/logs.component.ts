import { Component, OnDestroy, OnInit } from '@angular/core';

import { LogsService } from '../../../shared/api/logs.service';

@Component({
  selector: 'cd-logs',
  templateUrl: './logs.component.html',
  styleUrls: ['./logs.component.scss']
})
export class LogsComponent implements OnInit, OnDestroy {
  contentData: any;
  interval: number;

  constructor(private logsService: LogsService) {}

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
    this.logsService.getLogs().subscribe((data: any) => {
      this.contentData = data;
    });
  }
}
