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
    expect(pipe.transform(value)).toBe('1 k');
  });

  it('transforms 1000^2', () => {
    const value = Math.pow(1000, 2);
    expect(pipe.transform(value)).toBe('1 M');
  });

  it('transforms 1000^3', () => {
    const value = Math.pow(1000, 3);
    expect(pipe.transform(value)).toBe('1 G');
  });

  it('transforms 1000^4', () => {
    const value = Math.pow(1000, 4);
    expect(pipe.transform(value)).toBe('1 T');
  });

  it('transforms 1000^5', () => {
    const value = Math.pow(1000, 5);
    expect(pipe.transform(value)).toBe('1 P');
  });

  it('transforms 1000^6', () => {
    const value = Math.pow(1000, 6);
    expect(pipe.transform(value)).toBe('1 E');
  });

  it('transforms 1000^7', () => {
    const value = Math.pow(1000, 7);
    expect(pipe.transform(value)).toBe('1 Z');
  });

  it('transforms 1000^8', () => {
    const value = Math.pow(1000, 8);
    expect(pipe.transform(value)).toBe('1 Y');
  });
});
