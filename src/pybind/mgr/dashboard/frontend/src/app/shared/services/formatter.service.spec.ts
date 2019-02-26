import { configureTestBed } from '../../../testing/unit-test-helper';
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

  configureTestBed({
    providers: [FormatterService, DimlessBinaryPipe]
  });

  beforeEach(() => {
    service = new FormatterService();
    dimlessBinaryPipe = new DimlessBinaryPipe(service);
    dimlessPipe = new DimlessPipe(service);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('format_number', () => {
    const formats = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];

    it('should return minus for unsupported values', () => {
      expect(service.format_number(service, 1024, formats)).toBe('-');
      expect(service.format_number(undefined, 1024, formats)).toBe('-');
      expect(service.format_number(null, 1024, formats)).toBe('-');
    });

    it('should test some values', () => {
      expect(service.format_number('0', 1024, formats)).toBe('0 B');
      expect(service.format_number('0.1', 1024, formats)).toBe('0.1 B');
      expect(service.format_number('1.2', 1024, formats)).toBe('1.2 B');
      expect(service.format_number('1', 1024, formats)).toBe('1 B');
      expect(service.format_number('1024', 1024, formats)).toBe('1 KiB');
      expect(service.format_number(23.45678 * Math.pow(1024, 3), 1024, formats)).toBe('23.5 GiB');
      expect(service.format_number(23.45678 * Math.pow(1024, 3), 1024, formats, 2)).toBe(
        '23.46 GiB'
      );
    });

    it('should test some dimless values', () => {
      expect(dimlessPipe.transform(0.6)).toBe('0.6');
      expect(dimlessPipe.transform(1000.608)).toBe('1 k');
      expect(dimlessPipe.transform(1e10)).toBe('10 G');
      expect(dimlessPipe.transform(2.37e16)).toBe('23.7 P');
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
      expect(service.toBytes(undefined)).toBeNull();
      expect(service.toBytes('')).toBeNull();
      expect(service.toBytes('-')).toBeNull();
      expect(service.toBytes(null)).toBeNull();
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
      convertToBytesAndBack('1.1 MiB');
      convertToBytesAndBack('1.0MiB', '1 MiB');
      convertToBytesAndBack('8.9 GiB');
      convertToBytesAndBack('123.5 EiB');
    });
  });
});
