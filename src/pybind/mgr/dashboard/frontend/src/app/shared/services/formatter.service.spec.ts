import { TestBed } from '@angular/core/testing';

import { DimlessBinaryPipe } from '../pipes/dimless-binary.pipe';
import { DimlessPipe } from '../pipes/dimless.pipe';
import { FormatterService } from './formatter.service';

describe('FormatterService', () => {
  let service: FormatterService;
  let dimlessBinaryPipe: DimlessBinaryPipe;
  let dimlessPipe: DimlessPipe;

  const convertToBytesAndBack = (value: string, newValue?: string) => {
    expect(dimlessBinaryPipe.transform(service.toBytes(value))).toBe(newValue || value);
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [FormatterService, DimlessBinaryPipe]
    });
    service = new FormatterService();
    dimlessBinaryPipe = new DimlessBinaryPipe(service);
    dimlessPipe = new DimlessPipe(service);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('truncate', () => {
    it('should do test integer values', () => {
      expect(service.truncate('1234', 8)).toBe('1234');
      expect(service.truncate(1234, 8)).toBe('1234');
    });

    it('should do test floating values', () => {
      const value = '1234.567899000';
      expect(service.truncate(value, 0)).toBe('1235');
      expect(service.truncate(value, 1)).toBe('1234.6');
      expect(service.truncate(value, 3)).toBe('1234.568');
      expect(service.truncate(value, 4)).toBe('1234.5679');
      expect(service.truncate(value, 5)).toBe('1234.5679');
      expect(service.truncate(value, 6)).toBe('1234.567899');
      expect(service.truncate(value, 7)).toBe('1234.567899');
      expect(service.truncate(value, 10)).toBe('1234.567899');
      expect(service.truncate(100.0, 4)).toBe('100');
    });
  });

  describe('format_number', () => {
    const formats = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];

    it('should return minus for unsupported values', () => {
      expect(service.format_number(service, 1024, formats)).toBe('-');
      expect(service.format_number(undefined, 1024, formats)).toBe('-');
      expect(service.format_number(null, 1024, formats)).toBe('-');
    });

    it('should test some values', () => {
      expect(service.format_number('0', 1024, formats)).toBe('0B');
      expect(service.format_number('0.1', 1024, formats)).toBe('0.1B');
      expect(service.format_number('1.2', 1024, formats)).toBe('1.2B');
      expect(service.format_number('1', 1024, formats)).toBe('1B');
      expect(service.format_number('1024', 1024, formats)).toBe('1KiB');
      expect(service.format_number(55.000001, 1000, formats, 1)).toBe('55B');
      expect(service.format_number(23.45678 * Math.pow(1024, 3), 1024, formats)).toBe('23.4568GiB');
    });

    it('should test some dimless values', () => {
      expect(dimlessPipe.transform(0.6)).toBe('0.6');
      expect(dimlessPipe.transform(1000.608)).toBe('1.0006k');
      expect(dimlessPipe.transform(1e10)).toBe('10G');
      expect(dimlessPipe.transform(2.37e16)).toBe('23.7P');
    });
  });

  describe('toBytes', () => {
    it('should not convert wrong values', () => {
      expect(service.toBytes('10xyz')).toBeNull();
      expect(service.toBytes('1.1.1KiB')).toBeNull();
      expect(service.toBytes('1.1 KiloByte')).toBeNull();
      expect(service.toBytes('1.1  kib')).toBeNull();
      expect(service.toBytes('1.kib')).toBeNull();
      expect(service.toBytes('1 ki')).toBeNull();
    });

    it('should convert values to bytes', () => {
      expect(service.toBytes('4815162342')).toBe(4815162342);
      expect(service.toBytes('100M')).toBe(104857600);
      expect(service.toBytes('100 M')).toBe(104857600);
      expect(service.toBytes('100 mIb')).toBe(104857600);
      expect(service.toBytes('100 mb')).toBe(104857600);
      expect(service.toBytes('100MIB')).toBe(104857600);
      expect(service.toBytes('1.532KiB')).toBe(Math.round(1.532 * 1024));
      expect(service.toBytes('0.000000000001TiB')).toBe(1);
    });

    it('should convert values to human readable again', () => {
      convertToBytesAndBack('1.1MiB');
      convertToBytesAndBack('1.0MiB', '1MiB');
      convertToBytesAndBack('8.9GiB');
      convertToBytesAndBack('123.456EiB');
    });
  });
});
