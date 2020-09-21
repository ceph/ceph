import { NotAvailablePipe } from './not-available.pipe';

describe('NotAvailablePipe', () => {
  let pipe: NotAvailablePipe;

  beforeEach(() => {
    pipe = new NotAvailablePipe();
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms not available', () => {
    expect(pipe.transform('')).toBe('n/a');
  });

  it('transforms number', () => {
    expect(pipe.transform(0)).toBe(0);
    expect(pipe.transform(1)).toBe(1);
  });
});
