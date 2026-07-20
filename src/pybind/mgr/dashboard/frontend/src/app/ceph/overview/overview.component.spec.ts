import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';

import { OverviewComponent } from './overview.component';
import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';

import { provideHttpClient } from '@angular/common/http';
import { provideRouter, RouterModule } from '@angular/router';

import { CommonModule } from '@angular/common';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { HealthMap, SeverityIconMap } from '~/app/shared/models/overview';
import { OverviewAlertsCardComponent } from './alerts-card/overview-alerts-card.component';
import { HardwareService } from '~/app/shared/api/hardware.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { OverviewStorageService } from '~/app/shared/api/storage-overview.service';
import { Component } from '@angular/core';

@Component({
  selector: 'cd-overview-alerts-card',
  standalone: true,
  template: ''
})
class MockOverviewAlertsCardComponent {}

describe('OverviewComponent', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;

  let mockHealthService: { getHealthSnapshot: jest.Mock };
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
  };

  const mockAuthStorageService = {
    getPermissions: jest.fn(() => ({ configOpt: { read: false } }))
  };

  const mockMgrModuleService = {
    getConfig: jest.fn(() => of({ hw_monitoring: false })),
    list: jest.fn(() => of([]))
  };

  const mockHardwareService = {
    getSummary: jest.fn(() => of(null))
  };

  beforeEach(async () => {
    mockHealthService = { getHealthSnapshot: jest.fn() };
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
      )
    };

    await TestBed.configureTestingModule({
      imports: [
        OverviewComponent,
        CommonModule,
        GridModule,
        TilesModule,
        OverviewStorageCardComponent,
        OverviewHealthCardComponent,
        OverviewAlertsCardComponent,
        RouterModule,
        HttpClientTestingModule
      ],
      providers: [
        provideHttpClient(),
        provideRouter([]),
        { provide: HealthService, useValue: mockHealthService },
        { provide: RefreshIntervalService, useValue: mockRefreshIntervalService },
        { provide: OverviewStorageService, useValue: mockOverviewStorageService },
        { provide: AuthStorageService, useValue: mockAuthStorageService },
        { provide: MgrModuleService, useValue: mockMgrModuleService },
        { provide: HardwareService, useValue: mockHardwareService }
      ]
    })
      .overrideComponent(OverviewComponent, {
        remove: {
          imports: [OverviewAlertsCardComponent]
        },
        add: {
          imports: [MockOverviewAlertsCardComponent]
        }
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

    let emitted = false;

    component.healthCardVm$.subscribe({
      next: () => (emitted = true),
      complete: () => {
        expect(emitted).toBe(false);
        done();
      }
    });

    mockRefreshIntervalService.intervalData$.next();
    mockRefreshIntervalService.intervalData$.complete();
  });

  it('storageCardVm$ should emit storage view model with mapped fields', (done) => {
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
      if (!vm.isBreakdownLoaded || !vm.averageDailyConsumption || !vm.estimatedTimeUntilFull) {
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
      expect(vm.averageDailyConsumption).toBe('12 GiB/day');
      expect(vm.estimatedTimeUntilFull).toBe('30 days');
      expect(vm.threshold).toBe(null);

      expect(mockOverviewStorageService.formatBytesForChart).toHaveBeenCalledWith(3236978688);
      expect(mockOverviewStorageService.mapStorageChartData).toHaveBeenCalled();

      sub.unsubscribe();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

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
    mockOverviewStorageService.getStorageBreakdown.mockReturnValue(of(null));

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
