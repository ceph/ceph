import { NotAvailablePipe } from './not-available.pipe';

describe('NotAvailablePipe', () => {
  let pipe: NotAvailablePipe;

  beforeEach(() => {
    pipe = new NotAvailablePipe();
  });

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms not available (1)', () => {
    expect(pipe.transform('')).toBe('n/a');
  });

  it('transforms not available (2)', () => {
    expect(pipe.transform('', 'Unknown')).toBe('Unknown');
  });

  it('transform not necessary (1)', () => {
    expect(pipe.transform(0)).toBe(0);
    expect(pipe.transform(1)).toBe(1);
  });

  it('transform not necessary (2)', () => {
    expect(pipe.transform('foo')).toBe('foo');
  });
});
