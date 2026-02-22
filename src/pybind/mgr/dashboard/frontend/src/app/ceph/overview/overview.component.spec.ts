import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';

import { OverviewComponent } from './overview.component';
import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';
import { provideHttpClient } from '@angular/common/http';
import { CommonModule } from '@angular/common';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { provideRouter, RouterModule } from '@angular/router';
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

  // -----------------------------
  // Component creation
  // -----------------------------
  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // -----------------------------
  // Vie model stream success
  // -----------------------------
  it('vm$ should emit transformed HealthSnapshotMap', (done) => {
    const mockData: HealthSnapshotMap = { health: { checks: { a: {} } } } as any;
    mockHealthService.getHealthSnapshot.mockReturnValue(of(mockData));

    component.vm$.subscribe((vm) => {
      expect(vm.healthData).toEqual(mockData);
      expect(vm.incidentCount).toBe(1);
      done();
    });

    mockRefreshIntervalService.intervalData$.next();
  });

  // -----------------------------
  // View model stream error → EMPTY
  // -----------------------------
  it('vm$ should not emit if healthService throws', (done) => {
    mockHealthService.getHealthSnapshot.mockReturnValue(throwError(() => new Error('API Error')));

    let emitted = false;

    component.vm$.subscribe({
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
    const destroy$ = (component as any).destroy$;
    const nextSpy = jest.spyOn(destroy$, 'next');
    const completeSpy = jest.spyOn(destroy$, 'complete');

    component.ngOnDestroy();

    expect(nextSpy).toHaveBeenCalled();
    expect(completeSpy).toHaveBeenCalled();
  });
});
