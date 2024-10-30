import { UpperFirstPipe } from './upper-first.pipe';

describe('UpperFirstPipe', () => {
  const pipe = new UpperFirstPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "foo"', () => {
    expect(pipe.transform('foo')).toEqual('Foo');
  });

  it('transforms "BAR"', () => {
    expect(pipe.transform('BAR')).toEqual('BAR');
  });
});
