import { CssHelper } from '~/app/shared/classes/css-helper';
import { HealthColorPipe } from '~/app/shared/pipes/health-color.pipe';

class CssHelperStub extends CssHelper {
  propertyValue(propertyName: string) {
    if (propertyName === 'health-color-healthy') {
      return 'fakeGreen';
    }
    if (propertyName === 'health-color-warning-wcag-aa-large-text') {
      return 'fakeOrange';
    }
    if (propertyName === 'health-color-error') {
      return 'fakeRed';
    }
    return '';
  }
}

describe('HealthColorPipe', () => {
  const pipe = new HealthColorPipe(new CssHelperStub());

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "HEALTH_OK"', () => {
    expect(pipe.transform('HEALTH_OK')).toEqual({
      color: 'fakeGreen'
    });
  });

  it('transforms "HEALTH_WARN"', () => {
    expect(pipe.transform('HEALTH_WARN')).toEqual({
      color: 'fakeOrange'
    });
  });

  it('transforms "HEALTH_ERR"', () => {
    expect(pipe.transform('HEALTH_ERR')).toEqual({
      color: 'fakeRed'
    });
  });

  it('transforms others', () => {
    expect(pipe.transform('abc')).toBe(null);
  });
});
