import { ChangeDetectionStrategy, Component, inject, OnDestroy } from '@angular/core';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { OverviewAlertsCardComponent } from './alerts-card/overview-alerts-card.component';
import { HealthService } from '~/app/shared/api/health.service';
import { HealthCheck, HealthSnapshotMap } from '~/app/shared/models/health.interface';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { catchError, exhaustMap, map, takeUntil } from 'rxjs/operators';
import { EMPTY, Observable, Subject } from 'rxjs';
import { CommonModule } from '@angular/common';

import { ComponentsModule } from '~/app/shared/components/components.module';
import { HealthIconMap } from '~/app/shared/models/overview';

interface OverviewVM {
  healthData: HealthSnapshotMap | null;
  incidentCount: number;
  checks: { name: string; description: string; icon: string }[];
}

@Component({
  selector: 'cd-overview',
  imports: [
    CommonModule,
    GridModule,
    TilesModule,
    OverviewStorageCardComponent,
    OverviewHealthCardComponent,
    ComponentsModule,
    OverviewAlertsCardComponent
  ],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewComponent implements OnDestroy {
  isHealthPanelOpen: boolean = false;

  private readonly healthService = inject(HealthService);
  private readonly refreshIntervalService = inject(RefreshIntervalService);

  private destroy$ = new Subject<void>();

  private healthData$: Observable<HealthSnapshotMap> = this.refreshIntervalObs(() =>
    this.healthService.getHealthSnapshot()
  );

  public vm$: Observable<OverviewVM> = this.healthData$.pipe(
    map((data: HealthSnapshotMap) => {
      const checks = data?.health?.checks ?? {};
      return {
        healthData: data,
        incidentCount: Object.keys(checks)?.length,
        checks: Object.entries(checks)?.map((check: [string, HealthCheck]) => ({
          name: check?.[0],
          description: check?.[1]?.summary?.message,
          icon: HealthIconMap[check?.[1]?.severity]
        }))
      };
    })
  );

  private refreshIntervalObs<T>(fn: () => Observable<T>): Observable<T> {
    return this.refreshIntervalService.intervalData$.pipe(
      exhaustMap(() => fn().pipe(catchError(() => EMPTY))),
      takeUntil(this.destroy$)
    );
  }

  togglePanel() {
    this.isHealthPanelOpen = !this.isHealthPanelOpen;
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
