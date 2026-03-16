import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { OverviewStorageCardComponent } from './overview-storage-card.component';
import { FormatterService } from '~/app/shared/services/formatter.service';

describe('OverviewStorageCardComponent', () => {
  let component: OverviewStorageCardComponent;
  let fixture: ComponentFixture<OverviewStorageCardComponent>;

  let mockFormatterService: {
    formatToBinary: jest.Mock;
  };

  beforeEach(async () => {
    mockFormatterService = {
      formatToBinary: jest.fn().mockReturnValue([10, 'GiB'])
    };

    await TestBed.configureTestingModule({
      imports: [OverviewStorageCardComponent, HttpClientTestingModule],
      providers: [{ provide: FormatterService, useValue: mockFormatterService }]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should set totalCapacity when valid value is provided', () => {
    component.totalCapacity = 1024;

    expect(component.totalRaw).toBe(10);
    expect(component.totalRawUnit).toBe('GiB');
    expect(mockFormatterService.formatToBinary).toHaveBeenCalledWith(1024, true);
  });

  it('should not set totalCapacity when formatter returns NaN', () => {
    mockFormatterService.formatToBinary.mockReturnValue([NaN, 'GiB']);

    component.totalCapacity = 1024;

    expect(component.totalRaw).toBeNull();
    expect(component.totalRawUnit).toBe('');
  });

  it('should set usedCapacity when valid value is provided', () => {
    component.usedCapacity = 2048;

    expect(component.usedRaw).toBe(10);
    expect(component.usedRawUnit).toBe('GiB');
    expect(mockFormatterService.formatToBinary).toHaveBeenCalledWith(2048, true);
  });

  it('should not set usedCapacity when formatter returns NaN', () => {
    mockFormatterService.formatToBinary.mockReturnValue([NaN, 'GiB']);

    component.usedCapacity = 2048;

    expect(component.usedRaw).toBeNull();
    expect(component.usedRawUnit).toBe('');
  });

  it('should not update chart options until both totalCapacity and usedCapacity are set', () => {
    mockFormatterService.formatToBinary.mockReturnValue([20, 'TiB']);

    component.totalCapacity = 1024;

    expect(component.options.meter.proportional.total).toBeNull();
    expect(component.options.meter.proportional.unit).toBe('');
    expect(component.options.tooltip).toBeUndefined();
  });

  it('should update chart options when both totalCapacity and usedCapacity are set', () => {
    mockFormatterService.formatToBinary
      .mockReturnValueOnce([20, 'TiB'])
      .mockReturnValueOnce([5, 'TiB']);

    component.totalCapacity = 1024;
    component.usedCapacity = 512;

    expect(component.options.meter.proportional.total).toBe(20);
    expect(component.options.meter.proportional.unit).toBe('TiB');
    expect(component.options.tooltip).toBeDefined();
    expect(typeof component.options.tooltip?.valueFormatter).toBe('function');
  });

  it('should use used unit in tooltip formatter', () => {
    mockFormatterService.formatToBinary
      .mockReturnValueOnce([20, 'TiB'])
      .mockReturnValueOnce([5, 'TiB']);

    component.totalCapacity = 1024;
    component.usedCapacity = 512;

    const formatter = component.options.tooltip?.valueFormatter as (value: number) => string;

    expect(formatter(12.3)).toBe('12.3 TiB');
  });

  it('should keep default input values for presentational fields', () => {
    expect(component.consumptionTrendData).toEqual([]);
    expect(component.averageDailyConsumption).toBe('');
    expect(component.estimatedTimeUntilFull).toBe('');
    expect(component.breakdownData).toEqual([]);
    expect(component.isBreakdownLoaded).toBe(false);
  });
});
