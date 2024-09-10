import { Component, Input, OnInit } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';

import { NgbCalendar, NgbDateStruct } from '@ng-bootstrap/ng-bootstrap';
import moment from 'moment';
import { Subscription } from 'rxjs';

@Component({
  selector: 'cd-date-time-picker',
  templateUrl: './date-time-picker.component.html',
  styleUrls: ['./date-time-picker.component.scss']
})
export class DateTimePickerComponent implements OnInit {
  @Input()
  control: UntypedFormControl;

  @Input()
  hasSeconds = true;

  @Input()
  hasTime = true;

  @Input()
  name = '';

  @Input()
  helperText = '';
  @Input()
  placeHolder = '';
  @Input()
  disabled = false;

  format: string;
  minDate: NgbDateStruct;
  datetime: {
    date: any;
    time: string;
    ampm: string;
  };
  date: { [key: number]: string }[] = [];
  time: string;
  ampm: string;
  sub: Subscription;
  @Input() defaultDate: boolean = false;
  constructor(private calendar: NgbCalendar) {}

  ngOnInit() {
    this.minDate = this.calendar.getToday();
    if (!this.hasTime) {
      this.format = 'YYYY-MM-DD';
    } else if (this.hasSeconds) {
      this.format = 'YYYY-MM-DD HH:mm:ss';
    } else {
      this.format = 'YYYY-MM-DD HH:mm';
    }

    let mom = moment(this.control?.value, this.format);

    if (!mom.isValid() || mom.isBefore(moment())) {
      mom = moment();
    }
    if (this.defaultDate) {
      this.date.push([]);
    } else {
      this.date.push(mom.format('YYYY-MM-DD'));
    }

    const time = mom.format('HH:mm:ss');
    this.time = mom.format('hh:mm');
    this.ampm = mom.hour() >= 12 ? 'PM' : 'AM';

    this.datetime = {
      date: this.date[0],
      time: time,
      ampm: this.ampm
    };

    this.onModelChange();
  }

  onModelChange(event?: any) {
    if (event) {
      if (event.length === 0) {
        this.datetime.date = { date: null, time: null, ampm: null };
      } else if (Array.isArray(event)) {
        this.datetime.date = moment(event[0]).format('YYYY-MM-DD');
      } else if (event && ['AM', 'PM'].includes(event)) {
        const initialMoment = moment(this.datetime.time, 'hh:mm:ss A');
        const updatedMoment = initialMoment.set(
          'hour',
          (initialMoment.hour() % 12) + (event === 'PM' ? 12 : 0)
        );
        this.datetime.time = moment(updatedMoment).format('HH:mm:ss');
        this.datetime.ampm = event;
      } else {
        const time = event;
        this.datetime.time = moment(`${this.datetime.date} ${time} ${this.datetime.ampm}`).format(
          'HH:mm:ss'
        );
      }
    }
    if (this.datetime) {
      const datetime = moment(`${this.datetime.date} ${this.datetime.time}`).format(this.format);

      setTimeout(() => {
        this.control.setValue(datetime);
      });
    } else {
      setTimeout(() => {
        this.control.setValue('');
      });
    }
  }
}
