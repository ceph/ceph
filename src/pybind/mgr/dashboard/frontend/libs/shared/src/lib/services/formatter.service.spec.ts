import { FormatterService } from './formatter.service';
import { FormControl } from '@angular/forms';

describe('FormatterService', () => {
  let service: FormatterService;

  beforeEach(() => {
    service = new FormatterService();
  });

  describe('format_number', () => {
    it('should format numbers with units', () => {
      expect(service.format_number(1024, 1024, ['B', 'KB', 'MB'], 2)).toBe('1 KB');
      expect(service.format_number('2048', 1024, ['B', 'KB', 'MB'], 1)).toBe('2 KB');
      expect(service.format_number(NaN, 1024, ['B', 'KB', 'MB'], 1)).toBe('N/A');
      expect(service.format_number('not a number', 1024, ['B', 'KB', 'MB'], 1)).toBe('-');
    });
  });

  describe('formatNumberFromTo', () => {
    it('should convert and format units correctly', () => {
      expect(service.formatNumberFromTo(1024, 'B', 'KB', 1024, ['B', 'KB', 'MB'], 2)).toBe('1 KB');
      expect(service.formatNumberFromTo('2048', 'B', 'KB', 1024, ['B', 'KB', 'MB'], 1)).toBe('2 KB');
      expect(service.formatNumberFromTo('not a number', 'B', 'KB', 1024, ['B', 'KB', 'MB'], 1)).toBe('-');
      expect(service.formatNumberFromTo(1024, 'B', 'GB', 1024, ['B', 'KB', 'MB'], 2)).toBe('1024 B');
      expect(service.formatNumberFromTo(1024, 'B', 'KB', 1024, null, ['B', 'KB', 'MB'], 2)).toBe('-');
    });
  });

  describe('toBytes', () => {
    it('should convert values to bytes', () => {
      expect(service.toBytes('1K')).toBe(1024);
      expect(service.toBytes('1M')).toBe(1024 * 1024);
      expect(service.toBytes('1G')).toBe(1024 * 1024 * 1024);
      expect(service.toBytes('1024B')).toBe(1024);
      expect(service.toBytes('invalid', 123)).toBe(123);
    });
  });

  describe('toMilliseconds', () => {
    it('should convert ms string to number', () => {
      expect(service.toMilliseconds('100ms')).toBe(100);
      expect(service.toMilliseconds('200')).toBe(200);
      expect(service.toMilliseconds('invalid')).toBe(0);
    });
  });

  describe('toIops', () => {
    it('should convert IOPS string to number', () => {
      expect(service.toIops('100IOPS')).toBe(100);
      expect(service.toIops('200')).toBe(200);
      expect(service.toIops('invalid')).toBe(0);
    });
  });

  describe('toOctalPermission', () => {
    it('should convert permission object to octal string', () => {
      const modes = {
        owner: ['read', 'write', 'execute'],
        group: ['read', 'execute'],
        others: ['read']
      };
      expect(service.toOctalPermission(modes)).toBe('754');
    });
  });

  describe('performValidation', () => {
    it('should return null for empty input', () => {
      const control = new FormControl('');
      expect(service.performValidation(control, '^\\d+$', { error: true })).toBeNull();
    });

    it('should return errorObject for invalid input', () => {
      const control = new FormControl('abc');
      expect(service.performValidation(control, '^\\d+$', { error: true })).toEqual({ error: true });
    });

    it('should return null for valid input', () => {
      const control = new FormControl('123');
      expect(service.performValidation(control, '^\\d+$', { error: true })).toBeNull();
    });

    it('should validate quota type', () => {
      const control = new FormControl('1B');
      expect(service.performValidation(control, '^\\d+B$', { error: true }, 'quota')).toEqual({ error: true });
      const control2 = new FormControl('2KB');
      expect(service.performValidation(control2, '^\\d+KB$', { error: true }, 'quota')).toBeNull();
    });
  });

  describe('iopmMaxSizeValidator', () => {
    it('should return null for empty input', () => {
      const control = new FormControl('');
      expect(service.iopmMaxSizeValidator(control)).toBeNull();
    });

    it('should return error for invalid input', () => {
      const control = new FormControl('abc');
      expect(service.iopmMaxSizeValidator(control)).toEqual({ rateOpsMaxSize: true });
    });

    it('should return error for input longer than 18 digits', () => {
      const control = new FormControl('1234567890123456789');
      expect(service.iopmMaxSizeValidator(control)).toEqual({ rateOpsMaxSize: true });
    });

    it('should return null for valid input', () => {
      const control = new FormControl('12345');
      expect(service.iopmMaxSizeValidator(control)).toBeNull();
    });
  });
});