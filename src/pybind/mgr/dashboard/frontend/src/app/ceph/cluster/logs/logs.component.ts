import { Component, OnDestroy, OnInit } from '@angular/core';

import { LogsService } from '../../../shared/api/logs.service';

@Component({
  selector: 'cd-logs',
  templateUrl: './logs.component.html',
  styleUrls: ['./logs.component.scss']
})
export class LogsComponent implements OnInit, OnDestroy {
  contentData: any;
  clog: Array<any>;
  audit_log: Array<any>;

  interval: number;
  bsConfig = {
    dateInputFormat: 'YYYY-MM-DD',
    containerClass: 'theme-default'
  };
  prioritys: Array<{ name: string; value: string }> = [
    { name: 'Info', value: '[INF]' },
    { name: 'Warning', value: '[WRN]' },
    { name: 'Error', value: '[ERR]' },
    { name: 'All', value: 'All' }
  ];
  priority = 'All';
  search = '';
  selectedDate: Date;
  startTime: Date = new Date();
  endTime: Date = new Date();
  constructor(private logsService: LogsService) {
    this.startTime.setHours(0, 0);
    this.endTime.setHours(23, 59);
  }

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
      this.filterLogs();
    });
  }

  abstractfilters(): any {
    const priority = this.priority;
    const key = this.search.toLowerCase().replace(/,/g, '');

    let yearMonthDay: string;
    if (this.selectedDate) {
      const m = this.selectedDate.getMonth() + 1;
      const d = this.selectedDate.getDate();

      const year = this.selectedDate.getFullYear().toString();
      const month = m <= 9 ? `0${m}` : `${m}`;
      const day = d <= 9 ? `0${d}` : `${d}`;
      yearMonthDay = `${year}-${month}-${day}`;
    } else {
      yearMonthDay = '';
    }

    const sHour = this.startTime ? this.startTime.getHours() : 0;
    const sMinutes = this.startTime ? this.startTime.getMinutes() : 0;
    const sTime = sHour * 60 + sMinutes;

    const eHour = this.endTime ? this.endTime.getHours() : 23;
    const eMinutes = this.endTime ? this.endTime.getMinutes() : 59;
    const eTime = eHour * 60 + eMinutes;

    return { priority, key, yearMonthDay, sTime, eTime };
  }

  filterExecutor(logs: Array<any>, filters: any): Array<any> {
    return logs.filter((line) => {
      const hour = parseInt(line.stamp.slice(11, 13), 10);
      const minutes = parseInt(line.stamp.slice(14, 16), 10);
      let prio: string, y_m_d: string, timeSpan: number;

      prio = filters.priority === 'All' ? line.priority : filters.priority;
      y_m_d = filters.yearMonthDay ? filters.yearMonthDay : line.stamp;
      timeSpan = hour * 60 + minutes;
      return (
        line.priority === prio &&
        line.message.toLowerCase().indexOf(filters.key) !== -1 &&
        line.stamp.indexOf(y_m_d) !== -1 &&
        timeSpan >= filters.sTime &&
        timeSpan <= filters.eTime
      );
    });
  }

  filterLogs() {
    const filters = this.abstractfilters();
    this.clog = this.filterExecutor(this.contentData.clog, filters);
    this.audit_log = this.filterExecutor(this.contentData.audit_log, filters);
  }

  clearSearchKey() {
    this.search = '';
    this.filterLogs();
  }
  clearDate() {
    this.selectedDate = null;
    this.filterLogs();
  }
}
