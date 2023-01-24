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
        value: this.timeToDate(5 * 60, 1)
      },
      {
        name: $localize`Last 15 minutes`,
        value: this.timeToDate(15 * 60, 3)
      },
      {
        name: $localize`Last 30 minutes`,
        value: this.timeToDate(30 * 60, 6)
      },
      {
        name: $localize`Last 1 hour`,
        value: this.timeToDate(3600, 12)
      },
      {
        name: $localize`Last 3 hours`,
        value: this.timeToDate(3 * 3600, 36)
      },
      {
        name: $localize`Last 6 hours`,
        value: this.timeToDate(6 * 3600, 72)
      },
      {
        name: $localize`Last 12 hours`,
        value: this.timeToDate(12 * 3600, 144)
      },
      {
        name: $localize`Last 24 hours`,
        value: this.timeToDate(24 * 3600, 288)
      },
      {
        name: $localize`Last 2 days`,
        value: this.timeToDate(48 * 3600, 576)
      },
      {
        name: $localize`Last 7 days`,
        value: this.timeToDate(168 * 3600, 2016)
      }
    ];
    this.time = this.times[3].value;
  }

  emitTime() {
    this.selectedTime.emit(this.timeToDate(this.time.end - this.time.start, this.time.step));
  }

  private timeToDate(secondsAgo: number, step: number): any {
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
