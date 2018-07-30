import { OsdSummaryPipe } from './osd-summary.pipe';

describe('OsdSummaryPipe', () => {
  const pipe = new OsdSummaryPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms with 1 up', () => {
    const value = {
      osds: [{ in: true, out: false }]
    };
    expect(pipe.transform(value)).toBe('1 (0 up, 1 in)');
  });

  it('transforms with 1 up and 1 in', () => {
    const value = {
      osds: [{ in: true, up: false }, { in: false, up: true }]
    };
    expect(pipe.transform(value)).toBe('2 (1 up, 1 in)');
  });
});
