import { HealthLabelPipe } from './health-label.pipe';

describe('HealthLabelPipe', () => {
  const pipe = new HealthLabelPipe();
  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "HEALTH_OK"', () => {
    expect(pipe.transform('HEALTH_OK')).toEqual('ok');
  });

  it('transforms "HEALTH_WARN"', () => {
    expect(pipe.transform('HEALTH_WARN')).toEqual('warning');
  });

  it('transforms "HEALTH_ERR"', () => {
    expect(pipe.transform('HEALTH_ERR')).toEqual('error');
  });

  it('transforms others', () => {
    expect(pipe.transform('abc')).toBe(null);
  });
});
