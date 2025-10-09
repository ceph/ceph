import { EmptyPipe } from './empty.pipe';

describe('EmptyPipe', () => {
  const pipe = new EmptyPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms with empty value', () => {
    expect(pipe.transform(undefined)).toBe('-');
  });

  it('transforms with some value', () => {
    const value = 'foo';
    expect(pipe.transform(value)).toBe('foo');
  });
});
