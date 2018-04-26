import { TestBed } from '@angular/core/testing';

import { FormatterService } from './formatter.service';

describe('FormatterService', () => {
  let service: FormatterService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [FormatterService]
    });
    service = new FormatterService();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should not convert 10xyz to bytes (failure)', () => {
    const bytes = service.toBytes('10xyz');
    expect(bytes).toBeNull();
  });

  it('should not convert 1.1.1KiB to bytes (failure)', () => {
    const bytes = service.toBytes('1.1.1KiB');
    expect(bytes).toBeNull();
  });

  it('should convert 4815162342 to bytes', () => {
    const bytes = service.toBytes('4815162342');
    expect(bytes).toEqual(jasmine.any(Number));
    expect(bytes).toBe(4815162342);
  });

  it('should convert 100M to bytes', () => {
    const bytes = service.toBytes('100M');
    expect(bytes).toEqual(jasmine.any(Number));
    expect(bytes).toBe(104857600);
  });

  it('should convert 1.532KiB to bytes', () => {
    const bytes = service.toBytes('1.532KiB');
    expect(bytes).toEqual(jasmine.any(Number));
    expect(bytes).toBe(Math.floor(1.532 * 1024));
  });

  it('should convert 0.000000000001TiB to bytes', () => {
    const bytes = service.toBytes('0.000000000001TiB');
    expect(bytes).toEqual(jasmine.any(Number));
    expect(bytes).toBe(1);
  });
});
