import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { OverviewStorageCardComponent } from './overview-storage-card.component';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { FormatterService } from '~/app/shared/services/formatter.service';

describe('OverviewStorageCardComponent (Jest)', () => {
  let component: OverviewStorageCardComponent;
  let fixture: ComponentFixture<OverviewStorageCardComponent>;

  let mockPrometheusService: {
    getPrometheusQueryData: jest.Mock;
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

  beforeEach(async () => {
    mockPrometheusService = {
      getPrometheusQueryData: jest.fn().mockReturnValue(of(mockPrometheusResponse))
    };

    mockFormatterService = {
      formatToBinary: jest.fn().mockReturnValue([10, 'GiB']),
      convertToUnit: jest.fn((value: number) => Number(value))
    };

    await TestBed.configureTestingModule({
      imports: [OverviewStorageCardComponent],
      providers: [
        { provide: PrometheusService, useValue: mockPrometheusService },
        { provide: FormatterService, useValue: mockFormatterService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewStorageCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // triggers ngOnInit
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
});
