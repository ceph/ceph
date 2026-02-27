import { MirrorHealthColorPipe } from './mirror-health-color.pipe';

describe('MirrorHealthColorPipe', () => {
  const pipe = new MirrorHealthColorPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "warning"', () => {
    expect(pipe.transform('warning')).toBe('tag-warning');
  });

  it('transforms "error"', () => {
    expect(pipe.transform('error')).toBe('tag-danger');
  });

  it('transforms "success"', () => {
    expect(pipe.transform('success')).toBe('tag-success');
  });

  it('transforms others', () => {
    expect(pipe.transform('abc')).toBe('tag-info');
  });
});
