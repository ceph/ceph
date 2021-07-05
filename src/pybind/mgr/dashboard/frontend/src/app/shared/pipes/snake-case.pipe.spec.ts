import { SnakeCasePipe } from '~/app/shared/pipes/snake-case.pipe';

describe('SnakeCasePipe', () => {
  const pipe = new SnakeCasePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should transforms [1]', () => {
    expect(pipe.transform('foo Bar')).toEqual('foo_bar');
  });

  it('should transforms [2]', () => {
    expect(pipe.transform('fooBar')).toEqual('foo_bar');
  });

  it('should transforms [3]', () => {
    expect(pipe.transform('--foo-Bar--')).toEqual('foo_bar');
  });
});
