import moment from 'moment';

import { CdDatePipe } from './cd-date.pipe';

describe('CdDatePipe', () => {
  let pipe = new CdDatePipe();

  it('create an instance', () => {
    pipe = new CdDatePipe();
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform('')).toBe('');
  });

  it('transforms with some date', () => {
    const result = moment
      .parseZone(moment.unix(1527085564486))
      .utc()
      .utcOffset(moment().utcOffset())
      .local()
      .format('D/M/YY hh:mm A');
    expect(pipe.transform(1527085564486)).toBe(result);
  });
});
