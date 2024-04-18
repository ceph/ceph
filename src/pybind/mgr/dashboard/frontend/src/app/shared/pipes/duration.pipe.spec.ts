import { DurationPipe } from './duration.pipe';

describe('DurationPipe', () => {
  const pipe = new DurationPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms seconds into a human readable duration', () => {
    expect(pipe.transform(0)).toBe('');
    expect(pipe.transform(6)).toBe('6 seconds');
    expect(pipe.transform(60)).toBe('1 minute');
    expect(pipe.transform(600)).toBe('10 minutes');
    expect(pipe.transform(6000)).toBe('1 hour 40 minutes');
  });
});
