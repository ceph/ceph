import { inject, TestBed } from '@angular/core/testing';

import { FormatterService } from './formatter.service';

describe('FormatterService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [FormatterService]
    });
  });

  it('should be created',
    inject([FormatterService], (service: FormatterService) => {
      expect(service).toBeTruthy();
    }));

  it('should not convert 10xyz to bytes (failure)',
    inject([FormatterService], (service: FormatterService) => {
      const bytes = service.toBytes('10xyz');
      expect(bytes).toBeNull();
    }));

  it('should convert 4815162342 to bytes',
    inject([FormatterService], (service: FormatterService) => {
      const bytes = service.toBytes('4815162342');
      expect(bytes).toEqual(jasmine.any(Number));
      expect(bytes).toBe(4815162342);
    }));

  it('should convert 100M to bytes',
    inject([FormatterService], (service: FormatterService) => {
      const bytes = service.toBytes('100M');
      expect(bytes).toEqual(jasmine.any(Number));
      expect(bytes).toBe(104857600);
    }));
});
