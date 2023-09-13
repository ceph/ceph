import { TruncatePipe } from './truncate.pipe';

describe('TruncatePipe', () => {
  const pipe = new TruncatePipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should truncate string (1)', () => {
    expect(pipe.transform('fsdfdsfs asdasd', 5, '')).toEqual('fsdfd');
  });

  it('should truncate string (2)', () => {
    expect(pipe.transform('fsdfdsfs asdasd', 10, '...')).toEqual('fsdfdsf...');
  });

  it('should not truncate number', () => {
    expect(pipe.transform(2, 6, '...')).toBe(2);
  });
});
