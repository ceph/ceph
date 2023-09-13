import { EncodeUriPipe } from './encode-uri.pipe';

describe('EncodeUriPipe', () => {
  it('create an instance', () => {
    const pipe = new EncodeUriPipe();
    expect(pipe).toBeTruthy();
  });

  it('should transforms the value', () => {
    const pipe = new EncodeUriPipe();
    expect(pipe.transform('rbd/name')).toBe('rbd%2Fname');
  });
});
