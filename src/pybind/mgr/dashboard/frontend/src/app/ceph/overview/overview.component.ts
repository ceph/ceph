import { CommonModule } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  inject,
  ViewEncapsulation
} from '@angular/core';
import { GridModule, LayoutModule, TilesModule } from 'carbon-components-angular';
import { EMPTY, Observable } from 'rxjs';
import { catchError, exhaustMap, map, shareReplay } from 'rxjs/operators';

import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthCheck, HealthSnapshotMap } from '~/app/shared/models/health.interface';
import {
  getClusterHealth,
  getHealthChecksAndIncidents,
  getResiliencyDisplay,
  HealthCardTabSection,
  HealthCardVM,
  HealthStatus,
  maxSeverity,
  safeDifference,
  SEVERITY,
  Severity,
  SeverityIconMap
} from '~/app/shared/models/overview';

import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { OverviewAlertsCardComponent } from './alerts-card/overview-alerts-card.component';
import { PerformanceCardComponent } from '~/app/shared/components/performance-card/performance-card.component';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';

/**
 * Mapper: HealthSnapshotMap -> HealthCardVM
 * Runs only when healthData$ emits.
 */
export function buildHealthCardVM(d: HealthSnapshotMap): HealthCardVM {
  const checksObj: Record<string, HealthCheck> = d.health?.checks ?? {};
  const clusterHealth = getClusterHealth(d.health.status as HealthStatus);
  const { incidents, checks } = getHealthChecksAndIncidents(checksObj);
  const resiliencyHealth = getResiliencyDisplay(checks);

  // --- System sub-states ---

  // MON
  const monTotal = d.monmap?.num_mons ?? 0;
  const monQuorum = (d.monmap as any)?.quorum?.length ?? 0;
  const monSev: Severity = monQuorum < monTotal ? SEVERITY.warn : SEVERITY.ok;

  // MGR
  const mgrActive = d.mgrmap?.num_active ?? 0;
  const mgrStandby = d.mgrmap?.num_standbys ?? 0;
  const mgrSev: Severity =
    mgrActive < 1 ? SEVERITY.err : mgrStandby < 1 ? SEVERITY.warn : SEVERITY.ok;

  // OSD
  const osdUp = (d.osdmap as any)?.up ?? 0;
  const osdIn = (d.osdmap as any)?.in ?? 0;
  const osdTotal = (d.osdmap as any)?.num_osds ?? 0;
  const osdDown = safeDifference(osdTotal, osdUp);
  const osdOut = safeDifference(osdTotal, osdIn);
  const osdSev: Severity = osdDown > 0 || osdOut > 0 ? SEVERITY.err : SEVERITY.ok;

  // HOSTS
  const hostsTotal = d.num_hosts ?? 0;
  const hostsAvailable = (d as any)?.num_hosts_available ?? 0;
  const hostsSev: Severity = hostsAvailable < hostsTotal ? SEVERITY.warn : SEVERITY.ok;

  // Overall = worst of the subsystem severities.
  const overallSystemSev = maxSeverity(monSev, mgrSev, osdSev, hostsSev);

  // Resiliency

  return {
    fsid: d.fsid,
    overallSystemSev: SeverityIconMap[overallSystemSev],

    incidents,
    checks,

    pgs: {
      total: d?.pgmap?.num_pgs,
      states: d?.pgmap?.pgs_by_state,
      io: [
        { label: $localize`Client write`, value: d?.pgmap?.write_bytes_sec },
        { label: $localize`Client read`, value: d?.pgmap?.read_bytes_sec },
        { label: $localize`Recovery I/O`, value: 0 }
      ]
    },

    clusterHealth,
    resiliencyHealth,

    mon: { value: $localize`Quorum: ${monQuorum}/${monTotal}`, severity: SeverityIconMap[monSev] },
    mgr: {
      value: $localize`${mgrActive} active, ${mgrStandby} standby`,
      severity: SeverityIconMap[mgrSev]
    },
    osd: { value: $localize`${osdUp}/${osdTotal} in/up`, severity: SeverityIconMap[osdSev] },
    hosts: {
      value: $localize`${hostsAvailable} / ${hostsTotal} available`,
      severity: SeverityIconMap[hostsSev]
    }
  };
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
    OverviewAlertsCardComponent,
    PerformanceCardComponent,
    LayoutModule,
    DataTableModule
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
  tableColumns = [
    { prop: 'count', name: $localize`PGs count` },
    { prop: 'state_name', name: $localize`Status` }
  ];

  private readonly healthService = inject(HealthService);
  private readonly refreshIntervalService = inject(RefreshIntervalService);
  private readonly destroyRef = inject(DestroyRef);

  private readonly healthData$: Observable<HealthSnapshotMap> = this.refreshIntervalObs(() =>
    this.healthService.getHealthSnapshot()
  );

  readonly healthCardVm$: Observable<HealthCardVM> = this.healthData$.pipe(
    map(buildHealthCardVM),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly storageVm$ = this.healthData$.pipe(
    map((data: HealthSnapshotMap) => ({
      total: data.pgmap?.bytes_total ?? 0,
      used: data.pgmap?.bytes_used ?? 0
    })),
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
