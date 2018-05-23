import { MdsSummaryPipe } from './mds-summary.pipe';

describe('MdsSummaryPipe', () => {
  const pipe = new MdsSummaryPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with 0 active and 2 standy', () => {
    const value = {
      standbys: [0],
      filesystems: [{ mdsmap: { info: [{ state: 'up:standby-replay' }] } }]
    };
    const result = { color: '#FF2222' };
    expect(pipe.transform(value)).toBe('0 active, 2 standby');
  });

  it('transforms with 1 active and 1 standy', () => {
    const value = {
      standbys: [0],
      filesystems: [{ mdsmap: { info: [{ state: 'up:active' }] } }]
    };
    const result = { color: '#FF2222' };
    expect(pipe.transform(value)).toBe('1 active, 1 standby');
  });

  it('transforms with 0 filesystems', () => {
    const value = {
      standbys: [0],
      filesystems: []
    };
    expect(pipe.transform(value)).toBe('no filesystems');
  });

  it('transforms without filesystem', () => {
    const value = { standbys: [0] };
    expect(pipe.transform(value)).toBe('1, no filesystems');
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });
});
