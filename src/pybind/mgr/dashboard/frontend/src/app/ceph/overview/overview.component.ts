import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, DestroyRef, inject } from '@angular/core';
import { GridModule, TilesModule } from 'carbon-components-angular';
import { EMPTY, Observable } from 'rxjs';
import { catchError, exhaustMap, map, shareReplay } from 'rxjs/operators';

import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthCheck, HealthSnapshotMap } from '~/app/shared/models/health.interface';
import {
  HealthCardCheckVM,
  HealthCardTabSection,
  HealthCardVM,
  HealthDisplayVM,
  HealthIconMap,
  HealthMap,
  HealthStatus,
  Severity,
  SeverityIconMap
} from '~/app/shared/models/overview';

import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { OverviewAlertsCardComponent } from './alerts-card/overview-alerts-card.component';
import { PerformanceCardComponent } from '~/app/shared/components/performance-card/performance-card.component';

const sev = {
  ok: 0 as Severity,
  warn: 1 as Severity,
  err: 2 as Severity
} as const;

const maxSeverity = (...values: Severity[]): Severity => Math.max(...values) as Severity;

function buildHealthDisplay(status: HealthStatus): HealthDisplayVM {
  return HealthMap[status] ?? HealthMap['HEALTH_OK'];
}

function safeDifference(a: number, b: number): number | null {
  return a != null && b != null ? a - b : null;
}

/**
 * Mapper: HealthSnapshotMap -> HealthCardVM
 * Runs only when healthData$ emits.
 */
export function buildHealthCardVM(d: HealthSnapshotMap): HealthCardVM {
  const checksObj: Record<string, HealthCheck> = d.health?.checks ?? {};
  const healthDisplay = buildHealthDisplay(d.health.status as HealthStatus);

  // --- Health panel ---

  // Count incidents
  let incidents = 0;
  const checks: HealthCardCheckVM[] = [];

  for (const [name, check] of Object.entries(checksObj)) {
    incidents++;
    checks.push({
      name,
      description: check?.summary?.message ?? '',
      icon: HealthIconMap[check?.severity] ?? ''
    });
  }

  // --- System sub-states ---

  // MON
  const monTotal = d.monmap?.num_mons ?? 0;
  const monQuorum = (d.monmap as any)?.quorum?.length ?? 0;
  const monSev: Severity = monQuorum < monTotal ? sev.warn : sev.ok;

  // MGR
  const mgrActive = d.mgrmap?.num_active ?? 0;
  const mgrStandby = d.mgrmap?.num_standbys ?? 0;
  const mgrSev: Severity = mgrActive < 1 ? sev.err : mgrStandby < 1 ? sev.warn : sev.ok;

  // OSD
  const osdUp = (d.osdmap as any)?.up ?? 0;
  const osdIn = (d.osdmap as any)?.in ?? 0;
  const osdTotal = (d.osdmap as any)?.num_osds ?? 0;
  const osdDown = safeDifference(osdTotal, osdUp);
  const osdOut = safeDifference(osdTotal, osdIn);
  const osdSev: Severity = osdDown > 0 || osdOut > 0 ? sev.err : sev.ok;

  // HOSTS
  const hostsTotal = d.num_hosts ?? 0;
  const hostsAvailable = (d as any)?.num_hosts_available ?? 0;
  const hostsSev: Severity = hostsAvailable < hostsTotal ? sev.warn : sev.ok;

  // Overall = worst of the subsystem severities.
  const overallSystemSev = maxSeverity(monSev, mgrSev, osdSev, hostsSev);

  return {
    fsid: d.fsid,
    overallSystemSev: SeverityIconMap[overallSystemSev],

    incidents,
    checks,

    health: healthDisplay,

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
    PerformanceCardComponent
  ],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewComponent {
  isHealthPanelOpen = false;
  activeHealthTab: HealthCardTabSection | null = null;

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

  togglePanel(): void {
    this.isHealthPanelOpen = !this.isHealthPanelOpen;
  }
}
