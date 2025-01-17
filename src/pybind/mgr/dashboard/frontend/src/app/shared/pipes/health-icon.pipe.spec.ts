import { HealthIconPipe } from './health-icon.pipe';

describe('HealthIconPipe', () => {
  const pipe = new HealthIconPipe();
  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "HEALTH_OK"', () => {
    expect(pipe.transform('HEALTH_OK')).toEqual('fa fa-check-circle');
  });

  it('transforms "HEALTH_WARN"', () => {
    expect(pipe.transform('HEALTH_WARN')).toEqual('fa fa-exclamation-triangle');
  });

  it('transforms "HEALTH_ERR"', () => {
    expect(pipe.transform('HEALTH_ERR')).toEqual('fa fa-exclamation-circle');
  });
});
