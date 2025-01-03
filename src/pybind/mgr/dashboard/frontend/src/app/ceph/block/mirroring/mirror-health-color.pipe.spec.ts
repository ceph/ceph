import { MirrorHealthColorPipe } from './mirror-health-color.pipe';

describe('MirrorHealthColorPipe', () => {
  const pipe = new MirrorHealthColorPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "warning"', () => {
    expect(pipe.transform('warning')).toBe('badge badge-warning');
  });

  it('transforms "error"', () => {
    expect(pipe.transform('error')).toBe('badge badge-danger');
  });

  it('transforms "success"', () => {
    expect(pipe.transform('success')).toBe('badge badge-success');
  });

  it('transforms others', () => {
    expect(pipe.transform('abc')).toBe('badge badge-info');
  });
});
