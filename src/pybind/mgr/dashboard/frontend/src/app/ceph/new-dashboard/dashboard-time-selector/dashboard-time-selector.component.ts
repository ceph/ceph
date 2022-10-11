import { Component, EventEmitter, Output } from '@angular/core';

import moment from 'moment';

@Component({
  selector: 'cd-dashboard-time-selector',
  templateUrl: './dashboard-time-selector.component.html',
  styleUrls: ['./dashboard-time-selector.component.scss']
})
export class DashboardTimeSelectorComponent {
  @Output()
  selectedTime = new EventEmitter<any>();

  times: any;
  time: any;

  constructor() {
    this.times = [
      {
        name: $localize`Last 5 minutes`,
        value: this.timeToDate(5 * 60)
      },
      {
        name: $localize`Last 15 minutes`,
        value: this.timeToDate(15 * 60)
      },
      {
        name: $localize`Last 30 minutes`,
        value: this.timeToDate(30 * 60)
      },
      {
        name: $localize`Last 1 hour`,
        value: this.timeToDate(3600)
      },
      {
        name: $localize`Last 3 hours`,
        value: this.timeToDate(3 * 3600)
      },
      {
        name: $localize`Last 6 hours`,
        value: this.timeToDate(6 * 3600, 30)
      },
      {
        name: $localize`Last 12 hours`,
        value: this.timeToDate(12 * 3600, 60)
      },
      {
        name: $localize`Last 24 hours`,
        value: this.timeToDate(24 * 3600, 120)
      },
      {
        name: $localize`Last 2 days`,
        value: this.timeToDate(48 * 3600, 300)
      },
      {
        name: $localize`Last 7 days`,
        value: this.timeToDate(168 * 3600, 900)
      }
    ];
    this.time = this.times[3].value;
  }

  emitTime() {
    this.selectedTime.emit(this.time);
  }

  private timeToDate(secondsAgo: number, step: number = 30): any {
    const date: number = moment().unix() - secondsAgo;
    const dateNow: number = moment().unix();
    const formattedDate: any = {
      start: date,
      end: dateNow,
      step: step
    };
    return formattedDate;
  }
}
