import { BooleanTextPipe } from './boolean-text.pipe';

describe('BooleanTextPipe', () => {
  let pipe: BooleanTextPipe;

  beforeEach(() => {
    pipe = new BooleanTextPipe();
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms true', () => {
    expect(pipe.transform(true)).toEqual('Yes');
  });

  it('transforms true, alternative text', () => {
    expect(pipe.transform(true, 'foo')).toEqual('foo');
  });

  it('transforms 1', () => {
    expect(pipe.transform(1)).toEqual('Yes');
  });

  it('transforms false', () => {
    expect(pipe.transform(false)).toEqual('No');
  });

  it('transforms false, alternative text', () => {
    expect(pipe.transform(false, 'foo', 'bar')).toEqual('bar');
  });

  it('transforms 0', () => {
    expect(pipe.transform(0)).toEqual('No');
  });
});
