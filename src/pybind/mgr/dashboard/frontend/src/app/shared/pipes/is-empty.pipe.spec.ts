import { IsEmptyPipe } from './is-empty.pipe';

describe('IsEmptyPipe', () => {
  const pipe = new IsEmptyPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should transform to true (1)', () => {
    expect(pipe.transform(NaN)).toBeTruthy();
  });

  it('should transform to true (2)', () => {
    expect(pipe.transform(undefined)).toBeTruthy();
  });

  it('should transform to true (3)', () => {
    expect(pipe.transform(null)).toBeTruthy();
  });

  it('should transform to true (4)', () => {
    expect(pipe.transform('')).toBeTruthy();
  });

  it('should transform to true (5)', () => {
    expect(pipe.transform({})).toBeTruthy();
  });

  it('should transform to true (6)', () => {
    expect(pipe.transform([])).toBeTruthy();
  });

  it('should transform to true (7)', () => {
    expect(pipe.transform(6)).toBeTruthy();
  });

  it('should transform to false (1)', () => {
    expect(pipe.transform('a')).toBeFalsy();
  });

  it('should transform to false (2)', () => {
    expect(pipe.transform([1])).toBeFalsy();
  });

  it('should transform to false (3)', () => {
    expect(pipe.transform({ foo: 'bar' })).toBeFalsy();
  });
});
