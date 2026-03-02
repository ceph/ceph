import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { OverviewStorageCardComponent } from './overview-storage-card.component';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DatePipe } from '@angular/common';

describe('OverviewStorageCardComponent (Jest)', () => {
  let component: OverviewStorageCardComponent;
  let fixture: ComponentFixture<OverviewStorageCardComponent>;

  let mockPrometheusService: {
    getPrometheusQueryData: jest.Mock;
    getRangeQueriesData: jest.Mock;
  };

  let mockFormatterService: {
    formatToBinary: jest.Mock;
    convertToUnit: jest.Mock;
  };

  const mockPrometheusResponse = {
    result: [
      {
        metric: { application: 'Block' },
        value: [0, '1024']
      },
      {
        metric: { application: 'Filesystem' },
        value: [0, '2048']
      },
      {
        metric: { application: 'Object' },
        value: [0, '0'] // should be filtered
      }
    ]
  };

  const mockRangePrometheusResponse = {
    result: [
      {
        metric: { application: 'Block' },
        values: [
          [0, '512'],
          [60, '1024']
        ]
      }
    ]
  };
  beforeEach(async () => {
    mockPrometheusService = {
      getPrometheusQueryData: jest.fn().mockReturnValue(of(mockPrometheusResponse)),
      getRangeQueriesData: jest.fn().mockReturnValue(of(mockRangePrometheusResponse))
    };

    mockFormatterService = {
      formatToBinary: jest.fn().mockReturnValue([10, 'GiB']),
      convertToUnit: jest.fn((value: number) => Number(value))
    };

    await TestBed.configureTestingModule({
      imports: [OverviewStorageCardComponent, HttpClientTestingModule],
      providers: [
        { provide: PrometheusService, useValue: mockPrometheusService },
        { provide: FormatterService, useValue: mockFormatterService },
        DatePipe
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  // --------------------------------------------------
  // CREATION
  // --------------------------------------------------

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // --------------------------------------------------
  // TOTAL setter (truthy)
  // --------------------------------------------------

  it('should set total when valid value provided', () => {
    component.total = 1024;

    expect(component.totalRaw).toBe(10);
    expect(component.totalRawUnit).toBe('GiB');
  });

  // --------------------------------------------------
  // TOTAL setter (falsy)
  // --------------------------------------------------

  it('should not set total when formatter returns NaN', () => {
    mockFormatterService.formatToBinary.mockReturnValue([NaN, 'GiB']);

    component.total = 0;

    expect(component.totalRaw).toBeUndefined();
  });

  // --------------------------------------------------
  // USED setter
  // --------------------------------------------------

  it('should set used correctly', () => {
    component.used = 2048;

    expect(component.usedRaw).toBe(10);
    expect(component.usedRawUnit).toBe('GiB');
  });
  // --------------------------------------------------
  // ngOnInit data load
  // --------------------------------------------------

  it('should load and filter data on init', () => {
    expect(mockPrometheusService.getPrometheusQueryData).toHaveBeenCalled();
    expect(component.allData.length).toBe(2); // Object filtered (0 value)
  });

  // --------------------------------------------------
  // FILTERING
  // --------------------------------------------------

  it('should filter displayData for selected storage type', () => {
    component.allData = [
      { group: 'Block', value: 10 },
      { group: 'Filesystem', value: 20 }
    ];

    component.onStorageTypeSelect({ item: { content: 'Block', selected: true } } as any);

    expect(component.displayData).toEqual([{ group: 'Block', value: 10 }]);
  });

  it('should show all data when ALL selected', () => {
    component.allData = [
      { group: 'Block', value: 10 },
      { group: 'Filesystem', value: 20 }
    ];

    component.onStorageTypeSelect({ item: { content: 'All', selected: true } } as any);

    expect(component.displayData.length).toBe(2);
  });

  // --------------------------------------------------
  // DROPDOWN
  // --------------------------------------------------

  it('should update storage type from dropdown selection', () => {
    component.onStorageTypeSelect({
      item: { content: 'Block', selected: true }
    });

    expect(component.selectedStorageType).toBe('Block');
  });

  it('should auto-select single item if only one exists', () => {
    component.allData = [{ group: 'Block', value: 10 }];

    (component as any)._setDropdownItemsAndStorageType();

    expect(component.selectedStorageType).toBe('All');
    expect(component.dropdownItems.length).toBe(2);
  });

  // --------------------------------------------------
  // DESTROY
  // --------------------------------------------------

  it('should clean up on destroy', () => {
    const nextSpy = jest.spyOn((component as any).destroy$, 'next');
    const completeSpy = jest.spyOn((component as any).destroy$, 'complete');

    component.ngOnDestroy();

    expect(nextSpy).toHaveBeenCalled();
    expect(completeSpy).toHaveBeenCalled();
  });

  // --------------------------------------------------
  // USED setter (falsy)
  // --------------------------------------------------

  it('should not set used when formatter returns NaN', () => {
    mockFormatterService.formatToBinary.mockReturnValue([NaN, 'GiB']);

    component.used = 0;

    expect(component.usedRaw).toBeUndefined();
  });

  // --------------------------------------------------
  // _getAllData
  // --------------------------------------------------

  it('should map Filesystem application to File system group', () => {
    mockFormatterService.convertToUnit.mockReturnValue(5);
    const data = {
      result: [{ metric: { application: 'Filesystem' }, value: [0, '1024'] }]
    };

    mockPrometheusService.getPrometheusQueryData.mockReturnValue(of(data));
    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.allData.some((d) => d.group === 'File system')).toBe(true);
  });

  it('should filter out entries with unknown application groups', () => {
    mockFormatterService.convertToUnit.mockReturnValue(5);
    const data = {
      result: [
        { metric: { application: 'Unknown' }, value: [0, '1024'] },
        { metric: { application: 'Block' }, value: [0, '2048'] }
      ]
    };

    mockPrometheusService.getPrometheusQueryData.mockReturnValue(of(data));
    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.allData.every((d) => d.group !== 'Unknown')).toBe(true);
  });

  it('should handle empty result in _getAllData', () => {
    mockPrometheusService.getPrometheusQueryData.mockReturnValue(of({ result: [] }));
    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.allData).toEqual([]);
  });

  it('should handle null data in _getAllData', () => {
    mockPrometheusService.getPrometheusQueryData.mockReturnValue(of(null));
    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.allData).toEqual([]);
  });

  // --------------------------------------------------
  // _setChartData
  // --------------------------------------------------

  it('should set displayUsedRaw to usedRaw when ALL is selected', () => {
    component.usedRaw = 42;
    component.allData = [{ group: 'Block', value: 10 }];
    component.selectedStorageType = 'All';

    (component as any)._setChartData();

    expect(component.displayUsedRaw).toBe(42);
  });

  it('should set displayUsedRaw to first matching value when specific type selected', () => {
    component.allData = [
      { group: 'Block', value: 15 },
      { group: 'File system', value: 25 }
    ];
    component.selectedStorageType = 'Block';

    (component as any)._setChartData();

    expect(component.displayUsedRaw).toBe(15);
  });

  it('should set displayData to empty array when no matching type found', () => {
    component.allData = [{ group: 'Block', value: 10 }];
    component.selectedStorageType = 'Object';

    (component as any)._setChartData();

    expect(component.displayData).toEqual([]);
  });

  // --------------------------------------------------
  // _setDropdownItemsAndStorageType
  // --------------------------------------------------

  it('should build dropdown items from allData', () => {
    component.allData = [
      { group: 'Block', value: 10 },
      { group: 'File system', value: 20 }
    ];

    (component as any)._setDropdownItemsAndStorageType();

    expect(component.dropdownItems).toEqual([
      { content: 'All' },
      { content: 'Block' },
      { content: 'File system' }
    ]);
  });

  it('should set only ALL dropdown item when allData is empty', () => {
    component.allData = [];

    (component as any)._setDropdownItemsAndStorageType();

    expect(component.dropdownItems).toEqual([{ content: 'All' }]);
  });

  // --------------------------------------------------
  // onStorageTypeSelect - non-ALL types
  // --------------------------------------------------

  it('should set topPoolsData to null when ALL is selected', () => {
    component.topPoolsData = [{ some: 'data' }];
    component.allData = [];

    component.onStorageTypeSelect({ item: { content: 'All', selected: true } });

    expect(component.topPoolsData).toBeNull();
  });

  it('should not call loadTopPools for ALL type', () => {
    const spy = jest.spyOn(component as any, 'loadTopPools');
    component.allData = [];

    component.onStorageTypeSelect({ item: { content: 'All', selected: true } });

    expect(spy).not.toHaveBeenCalled();
  });

  it('should call loadTopPools when non-ALL type is selected', () => {
    const spy = jest.spyOn(component as any, 'loadTopPools').mockImplementation(() => {});
    jest.spyOn(component as any, 'loadCounts').mockImplementation(() => {});
    component.allData = [{ group: 'Block', value: 10 }];

    component.onStorageTypeSelect({ item: { content: 'Block', selected: true } });

    expect(spy).toHaveBeenCalled();
  });

  it('should call loadCounts when non-ALL type is selected', () => {
    jest.spyOn(component as any, 'loadTopPools').mockImplementation(() => {});
    const spy = jest.spyOn(component as any, 'loadCounts').mockImplementation(() => {});
    component.allData = [{ group: 'Block', value: 10 }];

    component.onStorageTypeSelect({ item: { content: 'Block', selected: true } });

    expect(spy).toHaveBeenCalled();
  });

  // --------------------------------------------------
  // ngOnInit - secondary calls
  // --------------------------------------------------

  it('should call loadTrend on init', () => {
    const spy = jest.spyOn(component as any, 'loadTrend').mockImplementation(() => {});

    component.ngOnInit();

    expect(spy).toHaveBeenCalled();
  });

  it('should call loadAverageConsumption on init', () => {
    const spy = jest.spyOn(component as any, 'loadAverageConsumption').mockImplementation(() => {});

    component.ngOnInit();

    expect(spy).toHaveBeenCalled();
  });

  it('should call loadTimeUntilFull on init', () => {
    const spy = jest.spyOn(component as any, 'loadTimeUntilFull').mockImplementation(() => {});

    component.ngOnInit();

    expect(spy).toHaveBeenCalled();
  });

  // --------------------------------------------------
  // _setTotalAndUsed / options update
  // --------------------------------------------------

  it('should update options.meter.proportional.total when total is set', () => {
    mockFormatterService.formatToBinary.mockReturnValue([20, 'TiB']);

    component.total = 1024 * 1024;

    expect(component.options.meter.proportional.total).toBe(20);
  });

  it('should update options.meter.proportional.unit when total is set', () => {
    mockFormatterService.formatToBinary.mockReturnValue([20, 'TiB']);

    component.total = 1024 * 1024;

    expect(component.options.meter.proportional.unit).toBe('TiB');
  });

  it('should set tooltip valueFormatter when used is set', () => {
    component.used = 512;

    expect(component.options.tooltip).toBeDefined();
    expect(typeof component.options.tooltip.valueFormatter).toBe('function');
  });

  // --------------------------------------------------
  // storageMetrics defaults
  // --------------------------------------------------

  it('should have default storageMetrics with zero values', () => {
    expect(component.storageMetrics.Block.metrics[0].value).toBe(0);
    expect(component.storageMetrics.Block.metrics[1].value).toBe(0);
    expect(component.storageMetrics['File system'].metrics[0].value).toBe(0);
    expect(component.storageMetrics['File system'].metrics[1].value).toBe(0);
    expect(component.storageMetrics.Object.metrics[0].value).toBe(0);
    expect(component.storageMetrics.Object.metrics[1].value).toBe(0);
  });
});
