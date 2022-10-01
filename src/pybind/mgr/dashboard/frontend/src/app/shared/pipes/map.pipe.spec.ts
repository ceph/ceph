import { MapPipe } from './map.pipe';

describe('MapPipe', () => {
  const pipe = new MapPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('map value [1]', () => {
    expect(pipe.transform('foo')).toBe('foo');
  });

  it('map value [2]', () => {
    expect(pipe.transform('foo', { '-1': 'disabled', 0: 'unlimited' })).toBe('foo');
  });

  it('map value [3]', () => {
    expect(pipe.transform(-1, { '-1': 'disabled', 0: 'unlimited' })).toBe('disabled');
  });

  it('map value [4]', () => {
    expect(pipe.transform(0, { '-1': 'disabled', 0: 'unlimited' })).toBe('unlimited');
  });
});
