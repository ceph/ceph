import { HealthColorPipe } from './health-color.pipe';

describe('HealthColorPipe', () => {
  const pipe = new HealthColorPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "HEALTH_OK"', () => {
    expect(pipe.transform('HEALTH_OK')).toEqual({ color: '#00bb00' });
  });

  it('transforms "HEALTH_WARN"', () => {
    expect(pipe.transform('HEALTH_WARN')).toEqual({ color: '#ffa500' });
  });

  it('transforms "HEALTH_ERR"', () => {
    expect(pipe.transform('HEALTH_ERR')).toEqual({ color: '#ff0000' });
  });

  it('transforms others', () => {
    expect(pipe.transform('abc')).toBe(null);
  });
});
