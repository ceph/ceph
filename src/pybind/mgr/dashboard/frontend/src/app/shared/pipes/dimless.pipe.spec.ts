import { FormatterService } from '../services/formatter.service';
import { DimlessPipe } from './dimless.pipe';

describe('DimlessPipe', () => {
  const formatterService = new FormatterService();
  const pipe = new DimlessPipe(formatterService);

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms 1000^0', () => {
    const value = Math.pow(1000, 0);
    expect(pipe.transform(value)).toBe('1');
  });

  it('transforms 1000^1', () => {
    const value = Math.pow(1000, 1);
    expect(pipe.transform(value)).toBe('1k');
  });

  it('transforms 1000^2', () => {
    const value = Math.pow(1000, 2);
    expect(pipe.transform(value)).toBe('1M');
  });

  it('transforms 1000^3', () => {
    const value = Math.pow(1000, 3);
    expect(pipe.transform(value)).toBe('1G');
  });

  it('transforms 1000^4', () => {
    const value = Math.pow(1000, 4);
    expect(pipe.transform(value)).toBe('1T');
  });

  it('transforms 1000^5', () => {
    const value = Math.pow(1000, 5);
    expect(pipe.transform(value)).toBe('1P');
  });

  it('transforms 1000^6', () => {
    const value = Math.pow(1000, 6);
    expect(pipe.transform(value)).toBe('1E');
  });

  it('transforms 1000^7', () => {
    const value = Math.pow(1000, 7);
    expect(pipe.transform(value)).toBe('1Z');
  });

  it('transforms 1000^8', () => {
    const value = Math.pow(1000, 8);
    expect(pipe.transform(value)).toBe('1Y');
  });
});
