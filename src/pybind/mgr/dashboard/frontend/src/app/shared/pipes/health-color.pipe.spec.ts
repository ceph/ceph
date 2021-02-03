import styles from '~/styles.scss';
import { HealthColorPipe } from './health-color.pipe';

describe('HealthColorPipe', () => {
  const pipe = new HealthColorPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "HEALTH_OK"', () => {
    expect(pipe.transform('HEALTH_OK')).toEqual({ color: styles.healthColorHealthy });
  });

  it('transforms "HEALTH_WARN"', () => {
    expect(pipe.transform('HEALTH_WARN')).toEqual({ color: styles.healthColorWarning });
  });

  it('transforms "HEALTH_ERR"', () => {
    expect(pipe.transform('HEALTH_ERR')).toEqual({ color: styles.healthColorError });
  });

  it('transforms others', () => {
    expect(pipe.transform('abc')).toBe(null);
  });
});
