import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MeterChartComponent } from '@carbon/charts-angular';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { OverviewStorageCardComponent } from './overview-storage-card.component';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { AreaChartComponent } from '~/app/shared/components/area-chart/area-chart.component';
import { OverviewStorageSavingsComponent } from '../storage-savings/overview-storage-savings.component';
import { SavingsCardVM, TrendPoint } from '~/app/shared/models/overview';

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
      imports: [OverviewStorageCardComponent],
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        { provide: FormatterService, useValue: mockFormatterService }
      ]
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
    expect(component.consumptionStats()).toEqual([]);
    expect(component.breakdownData).toEqual([]);
    expect(component.isBreakdownLoaded).toBe(false);
  });

  describe('tabs', () => {
    const trend: TrendPoint[] = [
      { timestamp: new Date('2024-01-01T00:00:00Z'), values: { Used: 1 } },
      { timestamp: new Date('2024-01-02T00:00:00Z'), values: { Used: 2 } }
    ];
    const savings: SavingsCardVM = {
      total: '7 TiB',
      trendData: trend,
      isTrendingUp: true,
      stats: [{ label: 'Compression', value: '2.4', description: 'ratio' }]
    };

    let element: HTMLElement;

    const render = (inputs: Record<string, unknown>) => {
      TestBed.resetTestingModule();
      return TestBed.configureTestingModule({
        imports: [OverviewStorageCardComponent],
        providers: [provideHttpClient(), provideHttpClientTesting()]
      })
        .overrideComponent(OverviewStorageCardComponent, {
          remove: {
            imports: [AreaChartComponent, MeterChartComponent, OverviewStorageSavingsComponent]
          },
          add: { schemas: [CUSTOM_ELEMENTS_SCHEMA] }
        })
        .compileComponents()
        .then(() => {
          fixture = TestBed.createComponent(OverviewStorageCardComponent);
          Object.entries({
            consumptionTrendData: trend,
            storageEmptyState: false,
            prometheusEmptyState: false,
            isSavingsLoaded: true,
            savings: null,
            ...inputs
          }).forEach(([name, value]) => fixture.componentRef.setInput(name, value));
          element = fixture.nativeElement;
          fixture.detectChanges();
        });
    };

    const tabHeadings = () =>
      Array.from(element.querySelectorAll('cds-tab-headers [role="tab"]')).map((tab) =>
        tab.textContent?.trim()
      );

    it('should show Savings first and Consumption second when savings are available', async () => {
      await render({ savings });

      expect(tabHeadings()).toEqual(['Savings', 'Consumption']);
      expect(element.querySelector('cd-overview-storage-savings')).not.toBeNull();
    });

    it('should show only the consumption trend, without tabs, when there are no savings', async () => {
      await render({});

      expect(element.querySelector('cds-tabs')).toBeNull();
      expect(element.querySelector('cd-overview-storage-savings')).toBeNull();
      expect(element.querySelector('cd-area-chart')).not.toBeNull();
    });

    it('should show skeleton tabs while savings are loading', async () => {
      await render({ isSavingsLoaded: false });

      expect(element.querySelector('cds-tabs-skeleton')).not.toBeNull();
      expect(element.querySelector('cd-area-chart')).toBeNull();
    });

    it('should show skeleton tabs until consumption data arrives', async () => {
      await render({ consumptionTrendData: [], savings });

      expect(element.querySelector('cds-tabs-skeleton')).not.toBeNull();
      expect(element.querySelector('cds-tabs')).toBeNull();
    });

    it.each(['storageEmptyState', 'prometheusEmptyState'])(
      'should not render tabs when %s is set',
      async (input) => {
        await render({ savings, [input]: true });

        expect(element.querySelector('cds-tabs')).toBeNull();
        expect(element.querySelector('cd-area-chart')).toBeNull();
      }
    );

    it('should pass the consumption stats to the stats list', async () => {
      await render({
        consumptionStats: [{ label: 'Average', value: '12 GiB/day', description: 'd' }]
      });

      expect(element.querySelector('cd-overview-stats')?.textContent).toContain('12 GiB/day');
    });
  });
});
