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

describe('OverviewComponent', () => {
  let component: OverviewComponent;
  let fixture: ComponentFixture<OverviewComponent>;

  let mockHealthService: { getHealthSnapshot: jest.Mock };
  let mockRefreshIntervalService: { intervalData$: Subject<void> };

  beforeEach(async () => {
    mockHealthService = { getHealthSnapshot: jest.fn() };
    mockRefreshIntervalService = { intervalData$: new Subject<void>() };

    await TestBed.configureTestingModule({
      imports: [
        OverviewComponent,
        CommonModule,
        GridModule,
        TilesModule,
        OverviewStorageCardComponent,
        OverviewHealthCardComponent,
        OverviewAlertsCardComponent,
        RouterModule
      ],
      providers: [
        provideHttpClient(),
        provideRouter([]),
        { provide: HealthService, useValue: mockHealthService },
        { provide: RefreshIntervalService, useValue: mockRefreshIntervalService },
        provideRouter([])
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(OverviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => jest.clearAllMocks());

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // -----------------------------
  // View model stream success
  // -----------------------------
  it('healthCardVm$ should emit HealthCardVM with new keys', (done) => {
    const mockData: HealthSnapshotMap = {
      fsid: 'fsid-123',
      health: {
        status: 'HEALTH_OK',
        checks: {
          a: { severity: 'HEALTH_WARN', summary: { message: 'A issue' } },
          b: { severity: 'HEALTH_ERR', summary: { message: 'B issue' } }
        }
      },
      // subsystem inputs used by mapper
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

      expect(vm.health).toEqual(HealthMap['HEALTH_OK']);

      expect(vm.mon).toEqual(
        expect.objectContaining({
          value: '3/3',
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
      monmap: { num_mons: 3, quorum: [0, 1, 2] } as any, // ok
      mgrmap: { num_active: 0, num_standbys: 0 } as any, // err (active < 1)
      osdmap: { num_osds: 2, up: 2, in: 2 } as any, // ok
      num_hosts: 1,
      num_hosts_down: 0 // ok
    } as any;

    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockData));

    const sub = component.healthCardVm$.subscribe((vm) => {
      // mgr -> err, therefore overall should be err icon
      expect(vm.overallSystemSev).toBe(SeverityIconMap[2]); // sev.err === 2
      sub.unsubscribe();
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

  // -----------------------------
  // View model stream error → EMPTY
  // -----------------------------
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

  // -----------------------------
  // toggle health panel
  // -----------------------------
  it('should toggle panel open/close', () => {
    expect(component.isHealthPanelOpen).toBe(false);
    component.togglePanel();
    expect(component.isHealthPanelOpen).toBe(true);
    component.togglePanel();
    expect(component.isHealthPanelOpen).toBe(false);
  });

  // -----------------------------
  // ngOnDestroy
  // -----------------------------
  it('should complete destroy$', () => {
    expect(() => fixture.destroy()).not.toThrow();
  });
});
