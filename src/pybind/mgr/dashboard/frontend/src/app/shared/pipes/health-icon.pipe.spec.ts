import { HealthIconPipe } from './health-icon.pipe';

describe('HealthIconPipe', () => {
  const pipe = new HealthIconPipe();
  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "HEALTH_OK"', () => {
    expect(pipe.transform('HEALTH_OK')).toEqual('checkmark--filled');
  });

  it('transforms "HEALTH_WARN"', () => {
    expect(pipe.transform('HEALTH_WARN')).toEqual('warning--alt--filled');
  });

  it('transforms "HEALTH_ERR"', () => {
    expect(pipe.transform('HEALTH_ERR')).toEqual('warning--filled');
  });
});
