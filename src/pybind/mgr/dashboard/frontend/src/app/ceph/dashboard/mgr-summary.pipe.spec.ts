import { MgrSummaryPipe } from './mgr-summary.pipe';

describe('MgrSummaryPipe', () => {
  const pipe = new MgrSummaryPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms with active_name undefined', () => {
    const value = {
      active_name: undefined,
      standbys: []
    };
    expect(pipe.transform(value)).toBe('active: n/a');
  });

  it('transforms with 1 active and 2 standbys', () => {
    const value = {
      active_name: 'a',
      standbys: ['b', 'c']
    };
    expect(pipe.transform(value)).toBe('active: a, 2 standbys');
  });
});
