import { JoinPipe } from './join.pipe';

describe('ListPipe', () => {
  const pipe = new JoinPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms "[1,2,3]"', () => {
    expect(pipe.transform([1, 2, 3])).toBe('1, 2, 3');
  });
});
