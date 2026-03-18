import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { OverviewStorageCardComponent } from './overview-storage-card.component';
import { FormatterService } from '~/app/shared/services/formatter.service';

describe('OverviewStorageCardComponent', () => {
  let component: OverviewStorageCardComponent;
  let fixture: ComponentFixture<OverviewStorageCardComponent>;

  let mockFormatterService: {
    formatToBinary: jest.Mock;
    convertToUnit: jest.Mock;
  };

  beforeEach(async () => {
    mockFormatterService = {
      formatToBinary: jest.fn((value: number) => {
        if (value === 1024) return [20, 'TiB'];
        if (value === 512) return [5, 'TiB'];
        if (value === 256) return [5, 'MiB'];
        return [10, 'GiB'];
      }),
      convertToUnit: jest.fn((value: number, fromUnit: string, toUnit: string) => {
        if (value === 20 && fromUnit === 'TiB' && toUnit === 'TiB') return 20;
        if (value === 20 && fromUnit === 'TiB' && toUnit === 'MiB') return 20;
        return value;
      })
    };

    await TestBed.configureTestingModule({
      imports: [OverviewStorageCardComponent, HttpClientTestingModule],
      providers: [{ provide: FormatterService, useValue: mockFormatterService }]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set totalCapacity when valid value is provided', () => {
    component.totalCapacity = 1024;

    expect(component.totalRaw).toBe(20);
    expect(component.totalRawUnit).toBe('TiB');
    expect(mockFormatterService.formatToBinary).toHaveBeenCalledWith(1024, true);
  });

  it('should not set totalCapacity when formatter returns NaN', () => {
    mockFormatterService.formatToBinary.mockReturnValue([NaN, 'GiB']);

    component.totalCapacity = 1024;

    expect(component.totalRaw).toBeNull();
    expect(component.totalRawUnit).toBe('');
  });

  it('should set usedCapacity when valid value is provided', () => {
    component.usedCapacity = 512;

    expect(component.usedRaw).toBe(5);
    expect(component.usedRawUnit).toBe('TiB');
    expect(mockFormatterService.formatToBinary).toHaveBeenCalledWith(512, true);
  });

  it('should not set usedCapacity when formatter returns NaN', () => {
    mockFormatterService.formatToBinary.mockReturnValue([NaN, 'GiB']);

    component.usedCapacity = 512;

    expect(component.usedRaw).toBeNull();
    expect(component.usedRawUnit).toBe('');
  });

  it('should not update chart options until both totalCapacity and usedCapacity are set', () => {
    component.totalCapacity = 1024;

    expect(component.options.meter.proportional.total).toBeNull();
    expect(component.options.meter.proportional.unit).toBe('');
    expect(component.options.tooltip).toBeUndefined();
  });

  it('should update chart options when both totalCapacity and usedCapacity are set', () => {
    component.totalCapacity = 1024;
    component.usedCapacity = 512;

    expect(mockFormatterService.convertToUnit).toHaveBeenCalledWith(20, 'TiB', 'TiB', 1);
    expect(component.options.meter.proportional.total).toBe(20);
    expect(component.options.meter.proportional.unit).toBe('TiB');
    expect(component.options.tooltip).toBeDefined();
    expect(typeof component.options.tooltip?.valueFormatter).toBe('function');
  });

  it('should use used unit in tooltip formatter', () => {
    mockFormatterService.formatToBinary.mockImplementation((value: number) => {
      if (value === 1024) return [20, 'TiB'];
      if (value === 512) return [5, 'MiB'];
      return [10, 'GiB'];
    });

    component.totalCapacity = 1024;
    component.usedCapacity = 512;

    const formatter = component.options.tooltip?.valueFormatter as (value: number) => string;

    expect(component.usedRawUnit).toBe('MiB');
    expect(component.options.meter.proportional.unit).toBe('MiB');
    expect(formatter(12.3)).toBe('12.3 MiB');
  });

  it('should keep default input values for presentational fields', () => {
    expect(component.consumptionTrendData).toEqual([]);
    expect(component.averageDailyConsumption).toBe('');
    expect(component.estimatedTimeUntilFull).toBe('');
    expect(component.breakdownData).toEqual([]);
    expect(component.isBreakdownLoaded).toBe(false);
  });
});
