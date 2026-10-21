import {
  ComponentFixture,
  discardPeriodicTasks,
  fakeAsync,
  TestBed,
  tick
} from '@angular/core/testing';
import { Observable, of, Subject, Subscription, throwError } from 'rxjs';

import { OverviewComponent } from './overview.component';
import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';

import { provideRouter } from '@angular/router';

import { HealthMap, SavingsCardVM, SeverityIconMap } from '~/app/shared/models/overview';
import { HardwareService } from '~/app/shared/api/hardware.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { OverviewStorageService } from '~/app/shared/api/storage-overview.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CompressionStats } from '~/app/shared/enum/hardware.enum';

describe('OverviewComponent', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;

  let mockHealthService: { getHealthSnapshot: jest.Mock; getTelemetryStatus: jest.Mock };
  let mockRefreshIntervalService: { intervalData$: Subject<void> };
  let mockOverviewStorageService: {
    getTrendData: jest.Mock;
    getAverageConsumption: jest.Mock;
    getTimeUntilFull: jest.Mock;
    getStorageBreakdown: jest.Mock;
    formatBytesForChart: jest.Mock;
    mapStorageChartData: jest.Mock;
    getThresholdStatus: jest.Mock;
    getRawCapacityThresholds: jest.Mock;
    getSavingsTrend: jest.Mock;
  };

  const mockAuthStorageService = {
    getPermissions: jest.fn(() => ({ configOpt: { read: false } })),
    isPwdDisplayedSource: new Subject<boolean>()
  };

  const mockMgrModuleService = {
    getConfig: jest.fn(() => of({ hw_monitoring: false })),
    list: jest.fn(() => of([]))
  };

  let hardwareMonitoringEnabled$: Observable<boolean>;
  const getCompression = jest.fn();
  const mockHardwareService = {
    getSummary: jest.fn(() => of(null)),
    get isHardwareMonitoringEnabled$() {
      return hardwareMonitoringEnabled$;
    },
    getCompression
  };

  let mockPrometheusService: {
    refreshPrometheusUsable: jest.Mock;
  };

  beforeEach(async () => {
    hardwareMonitoringEnabled$ = of(false);
    getCompression.mockReset();
    mockPrometheusService = {
      refreshPrometheusUsable: jest.fn().mockReturnValue(of(true))
    };

    mockHealthService = {
      getHealthSnapshot: jest.fn(),
      getTelemetryStatus: jest.fn().mockReturnValue(of(false))
    };
    mockRefreshIntervalService = { intervalData$: new Subject<void>() };

    mockOverviewStorageService = {
      getTrendData: jest.fn().mockReturnValue(
        of({
          TOTAL_RAW_USED: [
            [0, '512'],
            [60, '1024']
          ]
        })
      ),
      getAverageConsumption: jest.fn().mockReturnValue(of('12 GiB/day')),
      getTimeUntilFull: jest.fn().mockReturnValue(of('30 days')),
      getStorageBreakdown: jest.fn().mockReturnValue(
        of({
          result: [
            { metric: { application: 'Block' }, value: [0, '1024'] },
            { metric: { application: 'Filesystem' }, value: [0, '2048'] }
          ]
        })
      ),
      formatBytesForChart: jest.fn().mockReturnValue([3, 'GiB']),
      mapStorageChartData: jest.fn().mockReturnValue([
        { group: 'Block', value: 1 },
        { group: 'File system', value: 2 }
      ]),
      getThresholdStatus: jest.fn().mockReturnValue(null),
      getRawCapacityThresholds: jest.fn().mockReturnValue(
        of({
          osdFullRatio: 0.99,
          osdNearfullRatio: 0.85
        })
      ),
      getSavingsTrend: jest.fn().mockReturnValue(
        of({
          SAVINGS: [
            [0, '1'],
            [60, '2']
          ]
        })
      )
    };

    await TestBed.configureTestingModule({
      imports: [OverviewComponent],
      providers: [
        provideRouter([]),
        { provide: HealthService, useValue: mockHealthService },
        { provide: RefreshIntervalService, useValue: mockRefreshIntervalService },
        { provide: OverviewStorageService, useValue: mockOverviewStorageService },
        { provide: PrometheusService, useValue: mockPrometheusService },
        { provide: AuthStorageService, useValue: mockAuthStorageService },
        { provide: MgrModuleService, useValue: mockMgrModuleService },
        { provide: HardwareService, useValue: mockHardwareService }
      ]
    })
      .overrideComponent(OverviewComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(OverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => jest.clearAllMocks());

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('healthCardVm$ should emit HealthCardVM correctly', (done) => {
    const mockData: HealthSnapshotMap = {
      fsid: 'fsid-123',
      health: {
        status: 'HEALTH_OK',
        checks: {
          a: { severity: 'HEALTH_WARN', summary: { message: 'A issue' } },
          b: { severity: 'HEALTH_ERR', summary: { message: 'B issue' } }
        }
      },
      pgmap: {
        pgs_by_state: [{ state_name: 'active+clean', count: 497 }],
        num_pools: 14,
        bytes_used: 3236978688,
        bytes_total: 325343772672,
        num_pgs: 497,
        write_bytes_sec: 0,
        read_bytes_sec: 0,
        recovering_bytes_per_sec: 0
      },
      monmap: { num_mons: 3, quorum: [0, 1, 2] } as any,
      mgrmap: { num_active: 1, num_standbys: 1 } as any,
      osdmap: { num_osds: 2, up: 2, in: 2 } as any,
      num_hosts: 5,
      num_hosts_down: 1
    } as any;

    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockData));

    const sub = component.healthCardVm$.subscribe((vm) => {
      expect(vm.fsid).toBe('fsid-123');
      expect(vm.incidents).toBe(2);

      expect(vm.checks).toHaveLength(2);
      expect(vm.checks[0]).toEqual(
        expect.objectContaining({
          name: 'a',
          description: 'A issue'
        })
      );
      expect(vm.checks[0].icon).toEqual(expect.any(String));

      expect(vm.clusterHealth).toEqual(HealthMap['HEALTH_OK']);

      expect(vm.mon).toEqual(
        expect.objectContaining({
          value: 'Quorum: 3/3',
          severity: expect.any(String)
        })
      );
      expect(vm.mgr).toEqual(
        expect.objectContaining({
          value: '1 active, 1 standby',
          severity: expect.any(String)
        })
      );
      expect(vm.osd).toEqual(
        expect.objectContaining({
          value: '2/2 in/up',
          severity: expect.any(String)
        })
      );
      expect(vm.hosts).toEqual(
        expect.objectContaining({
          value: '0 / 5 available',
          severity: expect.any(String)
        })
      );

      expect(vm.overallSystemSev).toEqual(expect.any(String));

      sub.unsubscribe();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

  it('healthCardVm$ should compute overallSystemSev as worst subsystem severity', (done) => {
    const mockData: HealthSnapshotMap = {
      fsid: 'fsid-999',
      health: { status: 'HEALTH_OK', checks: {} },
      monmap: { num_mons: 3, quorum: [0, 1, 2] } as any,
      mgrmap: { num_active: 0, num_standbys: 0 } as any,
      osdmap: { num_osds: 2, up: 2, in: 2 } as any,
      pgmap: {
        pgs_by_state: [{ state_name: 'active+clean', count: 497 }],
        num_pools: 14,
        bytes_used: 3236978688,
        bytes_total: 325343772672,
        num_pgs: 497,
        write_bytes_sec: 0,
        read_bytes_sec: 0,
        recovering_bytes_per_sec: 0
      },
      num_hosts: 1,
      num_hosts_down: 0
    } as any;

    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockData));

    const sub = component.healthCardVm$.subscribe((vm) => {
      expect(vm.overallSystemSev).toBe(SeverityIconMap[2]);
      sub.unsubscribe();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

  it('healthCardVm$ should not emit if healthService throws (EMPTY)', (done) => {
    mockHealthService.getHealthSnapshot.mockReturnValue(throwError(() => new Error('API Error')));

    let emittedStates = false;

    component.healthCardVm$.subscribe({
      next: () => (emittedStates = true),
      complete: () => {
        expect(emittedStates).toBe(false);
        done();
      }
    });

    mockRefreshIntervalService.intervalData$.next();
    mockRefreshIntervalService.intervalData$.complete();
  });

  it('storageCardVm$ should emit storage view model with mapped fields', fakeAsync((done) => {
    const mockData: HealthSnapshotMap = {
      fsid: 'fsid-storage',
      health: { status: 'HEALTH_OK', checks: {} },
      pgmap: {
        pgs_by_state: [{ state_name: 'active+clean', count: 497 }],
        num_pools: 14,
        bytes_used: 3236978688,
        bytes_total: 325343772672,
        num_pgs: 497,
        write_bytes_sec: 0,
        read_bytes_sec: 0,
        recovering_bytes_per_sec: 0
      },
      monmap: { num_mons: 3, quorum: [0, 1, 2] } as any,
      mgrmap: { num_active: 1, num_standbys: 1 } as any,
      osdmap: { num_osds: 2, up: 2, in: 2 } as any,
      num_hosts: 5,
      num_hosts_down: 0
    } as any;

    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockData));

    const sub = component.storageCardVm$.subscribe((vm) => {
      if (!vm.isBreakdownLoaded || !vm.consumptionStats[0].value || !vm.consumptionStats[1].value) {
        return;
      }
      expect(vm.totalCapacity).toBe(325343772672);
      expect(vm.usedCapacity).toBe(3236978688);
      expect(vm.breakdownData).toEqual([
        { group: 'Block', value: 1 },
        { group: 'File system', value: 2 }
      ]);
      expect(vm.isBreakdownLoaded).toBe(true);
      expect(vm.consumptionTrendData).toEqual([
        {
          timestamp: new Date(0),
          values: { Used: 512 }
        },
        {
          timestamp: new Date(60000),
          values: { Used: 1024 }
        }
      ]);
      expect(vm.consumptionStats.map((stat) => stat.value)).toEqual(['30 days', '12 GiB/day']);
      expect(vm.threshold).toBe(null);

      expect(mockOverviewStorageService.formatBytesForChart).toHaveBeenCalledWith(3236978688);
      expect(mockOverviewStorageService.mapStorageChartData).toHaveBeenCalled();

      sub.unsubscribe();
      done();
    });

    tick(0);
    mockRefreshIntervalService.intervalData$.next();
    discardPeriodicTasks();
  }));

  it('storageCardVm$ should emit safe defaults before storage side streams resolve', (done) => {
    const mockData: HealthSnapshotMap = {
      fsid: 'fsid-storage',
      health: { status: 'HEALTH_OK', checks: {} },
      pgmap: {
        pgs_by_state: [{ state_name: 'active+clean', count: 1 }],
        num_pools: 1,
        bytes_used: 100,
        bytes_total: 1000,
        num_pgs: 1,
        write_bytes_sec: 0,
        read_bytes_sec: 0,
        recovering_bytes_per_sec: 0
      },
      monmap: { num_mons: 1, quorum: [0] } as any,
      mgrmap: { num_active: 1, num_standbys: 1 } as any,
      osdmap: { num_osds: 1, up: 1, in: 1 } as any,
      num_hosts: 1,
      num_hosts_down: 0
    } as any;

    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockData));
    mockOverviewStorageService.getStorageBreakdown.mockReturnValue(of({ result: [] }));

    const sub = component.storageCardVm$.subscribe((vm) => {
      expect(vm.totalCapacity).toBe(1000);
      expect(vm.usedCapacity).toBe(100);
      expect(vm.breakdownData).toEqual([]);
      expect(vm.isBreakdownLoaded).toBe(false);

      sub.unsubscribe();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

  describe('savingsState$', () => {
    const compressionStats: CompressionStats = {
      fcm_drive_count: 4,
      phy_util_percent: 38.5,
      log_util_percent: 61.2,
      savings_bytes: 1024,
      compression_ratio: 2.4,
      efficiency_percent: 58.3
    };

    let emittedStates: { isSavingsLoaded: boolean; savings: SavingsCardVM | null }[];
    let subscription: Subscription;

    const subscribeToSavings = (enabled = true) => {
      hardwareMonitoringEnabled$ = of(enabled);
      fixture = TestBed.createComponent(OverviewComponent);
      emittedStates = [];
      subscription = fixture.componentInstance.savingsState$.subscribe((state) =>
        emittedStates.push(state)
      );
    };
    const emitRefreshTick = () => mockRefreshIntervalService.intervalData$.next();
    const latestState = () => emittedStates[emittedStates.length - 1];

    afterEach(() => subscription?.unsubscribe());

    it('should start as not loaded', () => {
      subscribeToSavings();

      expect(emittedStates[0]).toEqual({ isSavingsLoaded: false, savings: null });
    });

    it.each([
      ['hardware monitoring is disabled', false, { ...compressionStats }],
      ['no FCM drives are detected', true, { ...compressionStats, fcm_drive_count: 0 }]
    ])('should be loaded without savings when %s', (_name, enabled, response) => {
      getCompression.mockReturnValue(of(response));

      subscribeToSavings(enabled);
      emitRefreshTick();

      expect(latestState()).toEqual({ isSavingsLoaded: true, savings: null });
      expect(mockOverviewStorageService.getSavingsTrend).not.toHaveBeenCalled();
    });

    it('should build the savings view model when FCM drives are present', fakeAsync(() => {
      getCompression.mockReturnValue(of(compressionStats));

      subscribeToSavings();
      emitRefreshTick();
      tick();

      const { isSavingsLoaded, savings } = latestState();
      expect(isSavingsLoaded).toBe(true);
      expect(savings.total).toBe('3 GiB');
      expect(savings.stats.map((stat) => stat.value)).toEqual(['2.4', '58.3%', '38.5%']);
      expect(savings.trendData.map((point) => point.values.Used)).toEqual([1, 2]);
      expect(savings.isTrendingUp).toBe(true);
      discardPeriodicTasks();
    }));

    it('should report no total when savings are not available yet', () => {
      getCompression.mockReturnValue(of({ ...compressionStats, savings_bytes: null }));

      subscribeToSavings();
      emitRefreshTick();

      expect(latestState().savings.total).toBeNull();
    });

    it('should not request the trend while Prometheus is not configured', fakeAsync(() => {
      mockPrometheusService.refreshPrometheusUsable.mockReturnValue(of(false));
      getCompression.mockReturnValue(of(compressionStats));

      subscribeToSavings();
      emitRefreshTick();
      tick();

      expect(mockOverviewStorageService.getSavingsTrend).not.toHaveBeenCalled();
      expect(latestState().savings.trendData).toEqual([]);
      discardPeriodicTasks();
    }));

    it('should keep the savings and drop the trend when the trend request fails', fakeAsync(() => {
      mockOverviewStorageService.getSavingsTrend.mockReturnValue(throwError(() => new Error('x')));
      getCompression.mockReturnValue(of(compressionStats));

      subscribeToSavings();
      emitRefreshTick();
      tick();

      expect(latestState().savings.trendData).toEqual([]);
      expect(latestState().savings.stats[0].value).toBe('2.4');
      discardPeriodicTasks();
    }));
  });

  it('should toggle panel open/close', () => {
    expect(component.isHealthPanelOpen).toBe(false);
    component.toggleHealthPanel();
    expect(component.isHealthPanelOpen).toBe(true);
    component.toggleHealthPanel();
    expect(component.isHealthPanelOpen).toBe(false);
  });

  it('should complete destroy$', () => {
    expect(() => fixture.destroy()).not.toThrow();
  });
});
