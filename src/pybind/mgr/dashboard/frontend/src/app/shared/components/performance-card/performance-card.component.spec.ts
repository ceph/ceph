import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { PerformanceCardComponent } from './performance-card.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { of } from 'rxjs';
import { PrometheusService } from '../../api/prometheus.service';
import { PerformanceCardService } from '../../api/performance-card.service';
import { PerformanceData } from '../../models/performance-data';
import { DatePipe } from '@angular/common';
import { NumberFormatterService } from '../../services/number-formatter.service';
import { AuthStorageService } from '../../services/auth-storage.service';
import { Permissions } from '../../models/permissions';

describe('PerformanceCardComponent', () => {
  let component: PerformanceCardComponent;
  let fixture: ComponentFixture<PerformanceCardComponent>;

  const mockChartData: PerformanceData = {
    iops: [{ timestamp: new Date(), values: { 'Read IOPS': 100, 'Write IOPS': 50 } }],
    latency: [{ timestamp: new Date(), values: { 'Read Latency': 1.5, 'Write Latency': 2.5 } }],
    throughput: [
      { timestamp: new Date(), values: { 'Read Throughput': 1000, 'Write Throughput': 500 } }
    ]
  };

  beforeEach(async () => {
    const prometheusServiceMock = {
      lastHourDateObject: { start: 1000, end: 2000, step: 14 }
    };

    const performanceCardServiceMock = {
      getChartData: jest.fn().mockReturnValue(of(mockChartData))
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

    const authStorageServiceMock = {
      getPermissions: jest.fn().mockReturnValue(new Permissions({ 'config-opt': ['read'] }))
    };

    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, PerformanceCardComponent],
      providers: [
        { provide: PrometheusService, useValue: prometheusServiceMock },
        { provide: PerformanceCardService, useValue: performanceCardServiceMock },
        { provide: NumberFormatterService, useValue: numberFormatterMock },
        { provide: DatePipe, useValue: datePipeMock },
        { provide: AuthStorageService, useValue: authStorageServiceMock }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(PerformanceCardComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call loadCharts on ngOnInit', () => {
    const loadChartsSpy = jest.spyOn(component, 'loadCharts');
    component.ngOnInit();
    expect(loadChartsSpy).toHaveBeenCalledWith(component.time);
  });

  it('should load charts and update chartDataSignal', fakeAsync(() => {
    const time = { start: 1000, end: 2000, step: 14 };

    component.loadCharts(time);
    tick();

    expect(component.chartDataSignal()).toEqual(mockChartData);
  }));

  it('should not load chart data when no storage', fakeAsync(() => {
    component.storageEmptyState = true;
    const time = { start: 1000, end: 2000, step: 14 };
    component.loadCharts(time);

    tick();

    expect(component.chartDataSignal()).toBeNull();
  }));

  it('should not load chart data when prometheus is disabled', fakeAsync(() => {
    component.prometheusEmptyState = true;
    const time = { start: 1000, end: 2000, step: 14 };
    component.loadCharts(time);

    tick();

    expect(component.chartDataSignal()).toBeNull();
  }));

  it('should cleanup subscriptions on ngOnDestroy', () => {
    const destroyNextSpy = jest.spyOn(component['destroy$'], 'next');
    const destroyCompleteSpy = jest.spyOn(component['destroy$'], 'complete');

    component.ngOnDestroy();

    expect(destroyNextSpy).toHaveBeenCalled();
    expect(destroyCompleteSpy).toHaveBeenCalled();
  });
});
