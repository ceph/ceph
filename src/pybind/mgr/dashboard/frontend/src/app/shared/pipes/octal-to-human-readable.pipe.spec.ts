import { OctalToHumanReadablePipe } from './octal-to-human-readable.pipe';

describe('OctalToHumanReadablePipe', () => {
  const testPipeResults = (value: any, expected: any) => {
    // eslint-disable-next-line
    for (let r in value) {
      expect(value[r].content).toEqual(expected[r].content);
    }
  };

  it('create an instance', () => {
    const pipe = new OctalToHumanReadablePipe();
    expect(pipe).toBeTruthy();
  });

  it('should transform decimal values to octal mode human readable', () => {
    const values = [16877, 16868, 16804];

    const expected = [
      [{ content: 'owner: rwx' }, { content: 'group: r-x' }, { content: 'others: r-x' }],
      [{ content: 'owner: rwx' }, { content: 'group: r--' }, { content: 'others: r--' }],
      [{ content: 'owner: rw-' }, { content: 'group: r--' }, { content: 'others: r--' }]
    ];

    const pipe = new OctalToHumanReadablePipe();
    // eslint-disable-next-line
    for (let index in values) {
      const summary = pipe.transform(values[index]);
      testPipeResults(summary, expected[index]);
    }
  });
});
