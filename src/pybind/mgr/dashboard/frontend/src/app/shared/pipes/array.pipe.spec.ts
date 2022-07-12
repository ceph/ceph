import { ArrayPipe } from './array.pipe';

describe('ArrayPipe', () => {
  const pipe = new ArrayPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms string to array', () => {
    expect(pipe.transform('foo')).toStrictEqual(['foo']);
  });

  it('transforms array to array', () => {
    expect(pipe.transform(['foo'], true)).toStrictEqual([['foo']]);
  });

  it('do not transforms array to array', () => {
    expect(pipe.transform(['foo'])).toStrictEqual(['foo']);
  });
});
