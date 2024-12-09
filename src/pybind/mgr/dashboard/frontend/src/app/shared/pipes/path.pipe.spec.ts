import { PathPipe } from './path.pipe';

describe('PathPipe', () => {
  it('create an instance', () => {
    const pipe = new PathPipe();
    expect(pipe).toBeTruthy();
  });

  it('should transform the path', () => {
    const pipe = new PathPipe();
    expect(pipe.transform('/a/b/c/d')).toBe('/a/.../d');
  });

  it('should transform the path with no slash at beginning', () => {
    const pipe = new PathPipe();
    expect(pipe.transform('a/b/c/d')).toBe('a/.../d');
  });
});
