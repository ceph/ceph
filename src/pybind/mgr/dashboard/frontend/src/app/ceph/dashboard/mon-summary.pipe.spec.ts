import { MonSummaryPipe } from './mon-summary.pipe';

describe('MonSummaryPipe', () => {
  const pipe = new MonSummaryPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms with 3 mons in quorum', () => {
    const value = {
      monmap: { mons: [0, 1, 2] },
      quorum: [0, 1, 2]
    };
    expect(pipe.transform(value)).toBe('3 (quorum 0, 1, 2)');
  });

  it('transforms with 2/3 mons in quorum', () => {
    const value = {
      monmap: { mons: [0, 1, 2] },
      quorum: [0, 1]
    };
    expect(pipe.transform(value)).toBe('3 (quorum 0, 1)');
  });
});
