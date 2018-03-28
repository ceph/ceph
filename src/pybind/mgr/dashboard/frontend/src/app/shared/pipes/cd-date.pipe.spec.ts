import { CdDatePipe } from './cd-date.pipe';

describe('CdDatePipe', () => {
  it('create an instance', () => {
    const pipe = new CdDatePipe(null);
    expect(pipe).toBeTruthy();
  });
});
