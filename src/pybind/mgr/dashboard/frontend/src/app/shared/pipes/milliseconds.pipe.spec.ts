import { MillisecondsPipe } from './milliseconds.pipe';

describe('MillisecondsPipe', () => {
  it('create an instance', () => {
    const pipe = new MillisecondsPipe();
    expect(pipe).toBeTruthy();
  });
});
