import { LogPriorityPipe } from './log-priority.pipe';

describe('LogPriorityPipe', () => {
  const pipe = new LogPriorityPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "INF"', () => {
    const value = '[INF]';
    const result = 'info';
    expect(pipe.transform(value)).toEqual(result);
  });

  it('transforms "WRN"', () => {
    const value = '[WRN]';
    const result = 'warn';
    expect(pipe.transform(value)).toEqual(result);
  });

  it('transforms "ERR"', () => {
    const value = '[ERR]';
    const result = 'err';
    expect(pipe.transform(value)).toEqual(result);
  });

  it('transforms others', () => {
    const value = '[foo]';
    expect(pipe.transform(value)).toBe('');
  });
});
