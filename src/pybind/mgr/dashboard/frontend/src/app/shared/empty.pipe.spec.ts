import { EmptyPipe } from './empty.pipe';

describe('EmptyPipe', () => {
  it('create an instance', () => {
    const pipe = new EmptyPipe();
    expect(pipe).toBeTruthy();
  });
});
