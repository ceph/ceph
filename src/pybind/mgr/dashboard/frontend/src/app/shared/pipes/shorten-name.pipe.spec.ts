import { ShortenNamePipe } from './shorten-name.pipe';

describe('ShortenNamePipe', () => {
  it('create an instance', () => {
    const pipe = new ShortenNamePipe();
    expect(pipe).toBeTruthy();
  });
});
