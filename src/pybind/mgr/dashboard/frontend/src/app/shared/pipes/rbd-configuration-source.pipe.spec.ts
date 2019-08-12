import { RbdConfigurationSourcePipe } from './rbd-configuration-source.pipe';

describe('RbdConfigurationSourcePipePipe', () => {
  let pipe: RbdConfigurationSourcePipe;

  beforeEach(() => {
    pipe = new RbdConfigurationSourcePipe();
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should transform correctly', () => {
    expect(pipe.transform('foo')).not.toBeDefined();
    expect(pipe.transform(-1)).not.toBeDefined();
    expect(pipe.transform(0)).toBe('global');
    expect(pipe.transform(1)).toBe('pool');
    expect(pipe.transform(2)).toBe('image');
    expect(pipe.transform(-3)).not.toBeDefined();
  });
});
