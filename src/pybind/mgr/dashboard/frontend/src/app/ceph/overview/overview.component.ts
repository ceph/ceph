import { CommonModule } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  inject,
  ViewEncapsulation
} from '@angular/core';
import { GridModule, LayoutModule, TilesModule } from 'carbon-components-angular';
import { combineLatest, EMPTY, Observable, timer } from 'rxjs';
import {
  catchError,
  distinctUntilChanged,
  exhaustMap,
  map,
  shareReplay,
  startWith,
  switchMap
} from 'rxjs/operators';

import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthSnapshotMap } from '~/app/shared/models/health.interface';
import {
  buildHealthCardVM,
  HealthCardTabSection,
  HealthCardVM,
  StorageCardVM
} from '~/app/shared/models/overview';

import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { OverviewAlertsCardComponent } from './alerts-card/overview-alerts-card.component';
import { PerformanceCardComponent } from '~/app/shared/components/performance-card/performance-card.component';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { OverviewStorageService } from '~/app/shared/api/storage-overview.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';

const SECONDS_PER_HOUR = 3600;
const SECONDS_PER_DAY = 86400;
const TREND_DAYS = 7;
const PROMETHUES_CONFIG_POLL_INTERVAL = 60000;

@Component({
  selector: 'cd-overview',
  imports: [
    CommonModule,
    GridModule,
    TilesModule,
    OverviewStorageCardComponent,
    OverviewHealthCardComponent,
    ComponentsModule,
    OverviewAlertsCardComponent,
    PerformanceCardComponent,
    LayoutModule,
    DataTableModule,
    PipesModule
  ],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None
})
export class OverviewComponent {
  isHealthPanelOpen = false;
  isPGStatePanelOpen = false;
  activeHealthTab: HealthCardTabSection | null = null;
  pgTableColumns = [
    { prop: 'count', name: $localize`PGs count` },
    { prop: 'state_name', name: $localize`Status` }
  ];
  hasOsd: boolean = false;

  private readonly healthService = inject(HealthService);
  private readonly refreshIntervalService = inject(RefreshIntervalService);
  private readonly overviewStorageService = inject(OverviewStorageService);
  private readonly destroyRef = inject(DestroyRef);
  private readonly prometheusService = inject(PrometheusService);

  /* HEALTH CARD DATA */
  private readonly healthData$: Observable<HealthSnapshotMap> = this.refreshIntervalObs(() =>
    this.healthService.getHealthSnapshot()
  ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

  readonly healthCardVm$: Observable<HealthCardVM> = this.healthData$.pipe(
    map(buildHealthCardVM),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  /* EMPTY STATE DATA */
  readonly isPromethuesConfigured$ = timer(0, PROMETHUES_CONFIG_POLL_INTERVAL).pipe(
    switchMap(() => this.prometheusService.refreshPrometheusUsable()),
    distinctUntilChanged(),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly hasNoOSDs$ = this.healthData$.pipe(
    map((data: HealthSnapshotMap) => (data?.osdmap?.num_osds ?? 0) === 0),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly storageEmptyState$ = this.hasNoOSDs$.pipe(startWith(false)).pipe(
    map((hasNoOSDs) => hasNoOSDs),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly prometheusEmptyState$ = this.isPromethuesConfigured$.pipe(
    map((isPromethuesConfigured) => !isPromethuesConfigured),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  /* STORAGE CARD DATA */
  readonly storageVm$ = this.healthData$.pipe(
    map((data: HealthSnapshotMap) => ({
      total: data.pgmap?.bytes_total,
      used: data.pgmap?.bytes_used
    })),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly averageConsumption$ = this.isPromethuesConfigured$.pipe(
    switchMap((isConfigured) =>
      isConfigured
        ? this.refreshIntervalObs(() => this.overviewStorageService.getAverageConsumption())
        : EMPTY
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly timeUntilFull$ = this.isPromethuesConfigured$.pipe(
    switchMap((isConfigured) =>
      isConfigured
        ? this.refreshIntervalObs(() => this.overviewStorageService.getTimeUntilFull())
        : EMPTY
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly breakdownRawData$ = this.isPromethuesConfigured$.pipe(
    switchMap((isConfigured) =>
      isConfigured
        ? this.refreshIntervalObs(() => this.overviewStorageService.getStorageBreakdown())
        : EMPTY
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly capacityThresholds$ = this.isPromethuesConfigured$.pipe(
    switchMap((isConfigured) =>
      isConfigured
        ? this.refreshIntervalObs(() => this.overviewStorageService.getRawCapacityThresholds())
        : EMPTY
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  // getTrendData() is already a polling stream through getRangeQueriesData()
  // hence no refresh needed.
  readonly trendData$ = this.isPromethuesConfigured$.pipe(
    switchMap((isConfigured) =>
      isConfigured
        ? this.overviewStorageService.getTrendData(
            Math.floor(Date.now() / 1000) - TREND_DAYS * SECONDS_PER_DAY,
            Math.floor(Date.now() / 1000),
            SECONDS_PER_HOUR
          )
        : EMPTY
    ),
    map((result) => {
      const values = result?.TOTAL_RAW_USED ?? [];

      return values.map(([ts, val]) => ({
        timestamp: new Date(ts * 1000),
        values: { Used: Number(val) }
      }));
    }),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly storageCardVm$: Observable<StorageCardVM> = combineLatest([
    this.storageVm$,
    this.breakdownRawData$.pipe(startWith(null)),
    this.trendData$.pipe(startWith([])),
    this.averageConsumption$.pipe(startWith('')),
    this.timeUntilFull$.pipe(startWith('')),
    this.capacityThresholds$.pipe(startWith({ osdFullRatio: null, osdNearfullRatio: null }))
  ]).pipe(
    map(
      ([
        storage,
        breakdownRawData,
        consumptionTrendData,
        averageDailyConsumption,
        estimatedTimeUntilFull,
        capacityThresholds
      ]) => {
        const total = storage?.total ?? 0;
        const used = storage?.used ?? 0;
        const [, unit] = this.overviewStorageService.formatBytesForChart(used);

        return {
          totalCapacity: total,
          usedCapacity: used,
          breakdownData: breakdownRawData
            ? this.overviewStorageService.mapStorageChartData(breakdownRawData, unit, used)
            : [],
          isBreakdownLoaded: !!breakdownRawData,
          consumptionTrendData,
          averageDailyConsumption,
          estimatedTimeUntilFull,
          threshold: this.overviewStorageService.getThresholdStatus(
            total,
            storage?.used,
            capacityThresholds.osdNearfullRatio,
            capacityThresholds.osdFullRatio
          )
        };
      }
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  private refreshIntervalObs<T>(fn: () => Observable<T>): Observable<T> {
    return this.refreshIntervalService.intervalData$.pipe(
      exhaustMap(() => fn().pipe(catchError(() => EMPTY))),
      takeUntilDestroyed(this.destroyRef)
    );
  }

  toggleHealthPanel(): void {
    this.isHealthPanelOpen = !this.isHealthPanelOpen;
  }

  togglePGStatesPanel(): void {
    this.isPGStatePanelOpen = !this.isPGStatePanelOpen;
  }
}
