import { TestBed } from '@angular/core/testing';
import { UsageService } from './usage.service';

describe('UsageService', () => {
  let service: UsageService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(UsageService);
  });

  describe('calculateUsedPercentage', () => {
    it('should return 0% if total is 0', () => {
      const result = service.calculateUsedPercentage(50, 0);
      expect(result).toBe(0);
    });

    it('should calculate the percentage correctly when total is greater than 0', () => {
      const result = service.calculateUsedPercentage(50, 200);
      expect(result).toBe(25);
    });

    it('should return 100% when used equals total', () => {
      const result = service.calculateUsedPercentage(200, 200);
      expect(result).toBe(100);
    });

    it('should return 0% when used is 0', () => {
      const result = service.calculateUsedPercentage(0, 100);
      expect(result).toBe(0);
    });
  });

  describe('getUsageInfo', () => {
    it('should return the correct used percentage and default color when thresholds are not provided', () => {
      const result = service.getUsageInfo(50, 200, undefined, undefined);
      expect(result.usedPercentage).toBe(25);
      expect(result.color).toBe('#0043ce');
    });

    it('should return a warning color if the used percentage exceeds the warning threshold', () => {
      const result = service.getUsageInfo(150, 200, 0.5, undefined);
      expect(result.usedPercentage).toBe(75);
      expect(result.color).toBe('#f1c21b');
    });

    it('should return an error color if the used percentage exceeds the error threshold', () => {
      const result = service.getUsageInfo(180, 200, 0.5, 0.8);
      expect(result.usedPercentage).toBe(90);
      expect(result.color).toBe('#da1e28');
    });

    it('should return default color if used percentage does not exceed any thresholds', () => {
      const result = service.getUsageInfo(99, 200, 0.5, 0.8); 
      expect(result.usedPercentage).toBe(49.5);
      expect(result.color).toBe('#0043ce');
    });
  });
});
