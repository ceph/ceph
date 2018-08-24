import { OsdSummaryPipe } from './osd-summary.pipe';

describe('OsdSummaryPipe', () => {
  const pipe = new OsdSummaryPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms without value', () => {
    expect(pipe.transform(undefined)).toBe('');
  });

  it('transforms having 1 with 0 up and 1 in', () => {
    const value = {
      osds: [{ in: true, out: false }]
    };
    expect(pipe.transform(value)).toEqual([
      {
        content: '1 (0 up, 1 in',
        style: { 'margin-right': '-5px', color: '' }
      },
      {
        content: ', ',
        style: { 'margin-right': '0', color: '' }
      },
      {
        content: '1 down',
        style: { 'margin-right': '-5px', color: OsdSummaryPipe.COLOR_ERROR }
      },
      {
        content: ')',
        style: { 'margin-right': '0', color: '' }
      }
    ]);
  });

  it('transforms having 2 with 2 up and 1 in', () => {
    const value = {
      osds: [{ in: true, up: true }, { in: false, up: true }]
    };
    expect(pipe.transform(value)).toEqual([
      {
        content: '2 (2 up, 1 in',
        style: { 'margin-right': '-5px', color: '' }
      },
      {
        content: ')',
        style: { 'margin-right': '0', color: '' }
      }
    ]);
  });
});
