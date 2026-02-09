import { Component, OnInit, OnDestroy } from '@angular/core';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { HealthService } from '~/app/shared/api/health.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { catchError, exhaustMap, takeUntil } from 'rxjs/operators';
import { EMPTY, Subject } from 'rxjs';

@Component({
  selector: 'cd-overview',
  imports: [GridModule, TilesModule, OverviewStorageCardComponent],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss'
})
export class OverviewComponent implements OnInit, OnDestroy {
  totalCapacity: number;
  usedCapacity: number;
  private destroy$ = new Subject<void>();

  constructor(
    private healthService: HealthService,
    private refreshIntervalService: RefreshIntervalService
  ) {}

  ngOnInit(): void {
    this.refreshIntervalObs(() => this.healthService.getHealthSnapshot()).subscribe({
      next: (healthData: HealthSnapshotMap) => {
        this.totalCapacity = healthData?.pgmap?.bytes_total;
        this.usedCapacity = healthData?.pgmap?.bytes_used;
      }
    });
  }

  refreshIntervalObs(fn: Function) {
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
