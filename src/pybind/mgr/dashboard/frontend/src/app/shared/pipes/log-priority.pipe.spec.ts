import { LogColorPipe } from './log-color.pipe';

describe('LogColorPipe', () => {
  const pipe = new LogColorPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "INF"', () => {
    const value = { priority: '[INF]' };
    expect(pipe.transform(value)).toBe('');
  });

  it('transforms "WRN"', () => {
    const value = { priority: '[WRN]' };
    const result = {
      color: '#ffa500',
      'font-weight': 'bold'
    };
    expect(pipe.transform(value)).toEqual(result);
  });

  it('transforms "ERR"', () => {
    const value = { priority: '[ERR]' };
    const result = { color: '#FF2222' };
    expect(pipe.transform(value)).toEqual(result);
  });

  it('transforms others', () => {
    const value = { priority: '[foo]' };
    expect(pipe.transform(value)).toBe('');
  });
});
