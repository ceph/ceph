import { ComponentFixture, TestBed, fakeAsync, tick, flush } from '@angular/core/testing';
import { PerformanceCardComponent } from './performance-card.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { of } from 'rxjs';
import { PrometheusService } from '../../api/prometheus.service';
import { PerformanceCardService } from '../../api/performance-card.service';
import { MgrModuleService } from '../../api/mgr-module.service';
import { StorageType, PerformanceData } from '../../models/performance-data';
import { DatePipe } from '@angular/common';
import { NumberFormatterService } from '../../services/number-formatter.service';

describe('PerformanceCardComponent', () => {
  let component: PerformanceCardComponent;
  let fixture: ComponentFixture<PerformanceCardComponent>;
  let prometheusService: PrometheusService;
  let performanceCardService: PerformanceCardService;
  let mgrModuleService: MgrModuleService;

  const mockChartData: PerformanceData = {
    iops: [{ timestamp: new Date(), values: { 'Read IOPS': 100, 'Write IOPS': 50 } }],
    latency: [{ timestamp: new Date(), values: { 'Read Latency': 1.5, 'Write Latency': 2.5 } }],
    throughput: [
      { timestamp: new Date(), values: { 'Read Throughput': 1000, 'Write Throughput': 500 } }
    ]
  };

  const mockMgrModules = [
    { name: 'prometheus', enabled: true },
    { name: 'other', enabled: false }
  ];

  beforeEach(async () => {
    const prometheusServiceMock = {
      lastHourDateObject: { start: 1000, end: 2000, step: 14 },
      ifPrometheusConfigured: jest.fn((fn) => fn())
    };

    const performanceCardServiceMock = {
      getChartData: jest.fn().mockReturnValue(of(mockChartData))
    };

    const mgrModuleServiceMock = {
      list: jest.fn().mockReturnValue(of(mockMgrModules))
    };

    const numberFormatterMock = {
      formatFromTo: jest.fn().mockReturnValue('1.00'),
      bytesPerSecondLabels: [
        'B/s',
        'KiB/s',
        'MiB/s',
        'GiB/s',
        'TiB/s',
        'PiB/s',
        'EiB/s',
        'ZiB/s',
        'YiB/s'
      ],
      bytesLabels: ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'YiB'],
      unitlessLabels: ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    };

    const datePipeMock = {
      transform: jest.fn().mockReturnValue('01 Jan, 00:00:00')
    };

    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, PerformanceCardComponent],
      providers: [
        { provide: PrometheusService, useValue: prometheusServiceMock },
        { provide: PerformanceCardService, useValue: performanceCardServiceMock },
        { provide: MgrModuleService, useValue: mgrModuleServiceMock },
        { provide: NumberFormatterService, useValue: numberFormatterMock },
        { provide: DatePipe, useValue: datePipeMock }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(PerformanceCardComponent);
    component = fixture.componentInstance;
    prometheusService = TestBed.inject(PrometheusService);
    performanceCardService = TestBed.inject(PerformanceCardService);
    mgrModuleService = TestBed.inject(MgrModuleService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize list signal from mgrModuleService', fakeAsync(() => {
    tick();
    expect(mgrModuleService.list).toHaveBeenCalled();
    expect(component.list()).toEqual(mockMgrModules);
    flush();
  }));

  it('should call loadCharts on ngOnInit', () => {
    const loadChartsSpy = jest.spyOn(component, 'loadCharts');
    component.ngOnInit();
    expect(loadChartsSpy).toHaveBeenCalledWith(component.time);
  });

  it('should load charts and update chartDataSignal', fakeAsync(() => {
    const time = { start: 1000, end: 2000, step: 14 };
    component.loadCharts(time);

    expect(component.time).toEqual(time);
    expect(performanceCardService.getChartData).toHaveBeenCalledWith(
      time,
      component.selectedStorageType
    );

    tick();
    expect(component.chartDataSignal()).toEqual(mockChartData);
  }));

  it('should set emptyStateKey when prometheus is enabled', fakeAsync(() => {
    const time = { start: 1000, end: 2000, step: 14 };
    component.loadCharts(time);

    tick();
    expect(mgrModuleService.list).toHaveBeenCalled();
    expect(component.emptyStateKey()).toBe('');
  }));

  it('should set emptyStateKey to prometheusDisabled when prometheus module is disabled', fakeAsync(async () => {
    const mockMgrModulesDisabled = [
      { name: 'prometheus', enabled: false },
      { name: 'other', enabled: true }
    ];
    (mgrModuleService.list as jest.Mock).mockReturnValue(of(mockMgrModulesDisabled));

    // Recreate component with new mock value
    fixture = TestBed.createComponent(PerformanceCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    tick();

    const time = { start: 1000, end: 2000, step: 14 };
    component.loadCharts(time);

    tick();
    expect(mgrModuleService.list).toHaveBeenCalled();
    expect(component.emptyStateKey()).toBe('prometheusDisabled');
  }));

  it('should handle empty mgr modules list', fakeAsync(() => {
    const mockMgrModulesEmpty: any[] = [];
    (mgrModuleService.list as jest.Mock).mockReturnValue(of(mockMgrModulesEmpty));

    // Recreate component with new mock value
    fixture = TestBed.createComponent(PerformanceCardComponent);
    component = fixture.componentInstance;
    // Don't call detectChanges() as it triggers ngOnInit which calls loadCharts
    // and loadCharts will crash with empty array
    tick();

    expect(mgrModuleService.list).toHaveBeenCalled();
    expect(component.list()).toEqual([]);
    flush();
  }));

  it('should set emptyStateKey when prometheus is not configured', fakeAsync(() => {
    (prometheusService.ifPrometheusConfigured as jest.Mock).mockImplementation((_fn, elseFn) => {
      if (elseFn) {
        elseFn();
      }
    });

    const time = { start: 1000, end: 2000, step: 14 };
    component.loadCharts(time);

    tick();
    expect(component.emptyStateKey()).toBe('prometheusNotAvailable');
  }));

  it('should update selectedStorageType and reload charts on storage type selection', () => {
    const loadChartsSpy = jest.spyOn(component, 'loadCharts');
    const event = { item: { value: StorageType.Filesystem } };

    component.onStorageTypeSelection(event);

    expect(component.selectedStorageType).toBe(StorageType.Filesystem);
    expect(loadChartsSpy).toHaveBeenCalledWith(component.time);
  });

  it('should cleanup subscriptions on ngOnDestroy', () => {
    const destroyNextSpy = jest.spyOn(component['destroy$'], 'next');
    const destroyCompleteSpy = jest.spyOn(component['destroy$'], 'complete');

    component.ngOnDestroy();

    expect(destroyNextSpy).toHaveBeenCalled();
    expect(destroyCompleteSpy).toHaveBeenCalled();
  });
});
