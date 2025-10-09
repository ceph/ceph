import moment from 'moment';

import { RelativeDatePipe } from './relative-date.pipe';

describe('RelativeDatePipe', () => {
  const pipe = new RelativeDatePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms date into a human readable relative time (1)', () => {
    const date: Date = moment().subtract(130, 'seconds').toDate();
    expect(pipe.transform(date)).toBe('2 minutes ago');
  });

  it('transforms date into a human readable relative time (2)', () => {
    const date: Date = moment().subtract(65, 'minutes').toDate();
    expect(pipe.transform(date)).toBe('An hour ago');
  });

  it('transforms date into a human readable relative time (3)', () => {
    const date: string = moment().subtract(130, 'minutes').toISOString();
    expect(pipe.transform(date)).toBe('2 hours ago');
  });

  it('transforms date into a human readable relative time (4)', () => {
    const date: string = moment().subtract(30, 'seconds').toISOString();
    expect(pipe.transform(date, false)).toBe('a few seconds ago');
  });

  it('transforms date into a human readable relative time (5)', () => {
    const date: number = moment().subtract(3, 'days').unix();
    expect(pipe.transform(date)).toBe('3 days ago');
  });

  it('invalid input (1)', () => {
    expect(pipe.transform('')).toBe('');
  });

  it('invalid input (2)', () => {
    expect(pipe.transform('2011-10-10T10:20:90')).toBe('');
  });
});
