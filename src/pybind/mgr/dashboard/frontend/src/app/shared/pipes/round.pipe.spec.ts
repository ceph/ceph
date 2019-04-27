import { RoundPipe } from './round.pipe';

describe('RoundPipe', () => {
  const pipe = new RoundPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "1500"', () => {
    expect(pipe.transform(1.52, 1)).toEqual(1.5);
  });
});
