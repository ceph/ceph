import { FormatterService } from '../services/formatter.service';
import { DimlessBinaryPipe } from './dimless-binary.pipe';

describe('DimlessBinaryPipe', () => {
  const formatterService = new FormatterService();
  const pipe = new DimlessBinaryPipe(formatterService);

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('transforms 1024^0', () => {
    const value = Math.pow(1024, 0);
    expect(pipe.transform(value)).toBe('1 B');
  });

  it('transforms 1024^1', () => {
    const value = Math.pow(1024, 1);
    expect(pipe.transform(value)).toBe('1 KiB');
  });

  it('transforms 1024^2', () => {
    const value = Math.pow(1024, 2);
    expect(pipe.transform(value)).toBe('1 MiB');
  });

  it('transforms 1024^3', () => {
    const value = Math.pow(1024, 3);
    expect(pipe.transform(value)).toBe('1 GiB');
  });

  it('transforms 1024^4', () => {
    const value = Math.pow(1024, 4);
    expect(pipe.transform(value)).toBe('1 TiB');
  });

  it('transforms 1024^5', () => {
    const value = Math.pow(1024, 5);
    expect(pipe.transform(value)).toBe('1 PiB');
  });

  it('transforms 1024^6', () => {
    const value = Math.pow(1024, 6);
    expect(pipe.transform(value)).toBe('1 EiB');
  });

  it('transforms 1024^7', () => {
    const value = Math.pow(1024, 7);
    expect(pipe.transform(value)).toBe('1 ZiB');
  });

  it('transforms 1024^8', () => {
    const value = Math.pow(1024, 8);
    expect(pipe.transform(value)).toBe('1 YiB');
  });
});
