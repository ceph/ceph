import { DatePipe } from '@angular/common';

import * as moment from 'moment';

import { CdDatePipe } from './cd-date.pipe';

describe('CdDatePipe', () => {
  const datePipe = new DatePipe('en-US');
  let pipe = new CdDatePipe(datePipe);

  it('create an instance', () => {
    pipe = new CdDatePipe(datePipe);
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform('')).toBe('');
  });

  it('transforms with some date', () => {
    const result = moment(1527085564486).format('M/D/YY LTS');
    expect(pipe.transform(1527085564486)).toBe(result);
  });
});
