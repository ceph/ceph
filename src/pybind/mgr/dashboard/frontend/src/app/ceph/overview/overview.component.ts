import { Component, OnDestroy } from '@angular/core';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { HealthService } from '~/app/shared/api/health.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { catchError, exhaustMap, takeUntil } from 'rxjs/operators';
import { EMPTY, Observable, Subject } from 'rxjs';
import { CommonModule } from '@angular/common';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';

@Component({
  selector: 'cd-overview',
  imports: [
    CommonModule,
    GridModule,
    TilesModule,
    OverviewStorageCardComponent,
    OverviewHealthCardComponent
  ],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent implements OnDestroy {
  private destroy$ = new Subject<void>();
  public healthData$: Observable<HealthSnapshotMap>;

  constructor(
    private healthService: HealthService,
    private refreshIntervalService: RefreshIntervalService
  ) {
    this.healthData$ = this.refreshIntervalObs<HealthSnapshotMap>(() =>
      this.healthService.getHealthSnapshot()
    );
  }

  refreshIntervalObs<T>(fn: () => Observable<T>): Observable<T> {
    return this.refreshIntervalService.intervalData$.pipe(
      exhaustMap(() => fn().pipe(catchError(() => EMPTY))),
      takeUntil(this.destroy$)
    );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
