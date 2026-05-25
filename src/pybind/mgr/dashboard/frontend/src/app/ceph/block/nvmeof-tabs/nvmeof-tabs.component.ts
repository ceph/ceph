import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Observable, Subject, forkJoin, of, timer } from 'rxjs';
import { catchError, map, shareReplay, switchMap, takeUntil } from 'rxjs/operators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import {
  NvmeofThroughput,
  PerformanceCardService
} from '~/app/shared/api/performance-card.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { NvmeofSubsystem } from '~/app/shared/models/nvmeof';
import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';
import { isNvmeofAlert, nvmeofAlertQueryParams } from '~/app/shared/helpers/nvmeof-alert.helper';

const NVMEOF_PATH = 'block/nvmeof';
const ALERT_POLL_INTERVAL = 30000;

export interface ResourceStats {
  gatewayGroups: number;
  gatewayGroupsDown: number;
  subsystems: number;
  namespaces: number;
  hosts: number;
  activeConnections: number;
  hasData: boolean;
}

export interface NvmeAlerts {
  critical: number;
  warning: number;
  total: number;
  byCategory: Record<string, number>;
}

enum TABS {
  gateways = 'gateways',
  subsystems = 'subsystems',
  namespaces = 'namespaces'
}

@Component({
  selector: 'cd-nvmeof-tabs',
  templateUrl: './nvmeof-tabs.component.html',
  styleUrls: ['./nvmeof-tabs.component.scss'],
  standalone: false
})
export class NvmeofTabsComponent implements OnInit, OnDestroy {
  @Input() showSetupCards = false;

  selectedTab: TABS | undefined;
  activeTab: TABS = TABS.gateways;
  nvmeof$: Observable<ResourceStats | null> = of(null);
  nvmeofThroughput$: Observable<NvmeofThroughput> = of({ reads: 0, writes: 0 });
  nvmeofAlerts$: Observable<NvmeAlerts> = of({
    critical: 0,
    warning: 0,
    total: 0,
    byCategory: {}
  });

  private destroy$ = new Subject<void>();

  constructor(
    private router: Router,
    private nvmeofService: NvmeofService,
    private performanceCardService: PerformanceCardService,
    private prometheusService: PrometheusService
  ) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.gateways;
    this.loadResourceStats();
    this.loadThroughput();
    this.loadAlerts();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  loadResourceStats(): void {
    this.nvmeof$ = this.nvmeofService.listGatewayGroups().pipe(
      switchMap((gatewayGroups: CephServiceSpec[][]) => {
        const firstItem = (gatewayGroups as any)?.[0];
        const rawGroups: CephServiceSpec[] = Array.isArray(firstItem)
          ? (firstItem as CephServiceSpec[])
          : Array.isArray(gatewayGroups)
            ? (gatewayGroups as unknown as CephServiceSpec[])
            : [];
        const groups = rawGroups.filter((g: CephServiceSpec) => g?.spec?.group);
        if (groups.length === 0) {
          return of(null);
        }
        const hostsSet = new Set<string>();
        groups.forEach((group: CephServiceSpec) => {
          (group.placement?.hosts ?? []).forEach((h: string) => hostsSet.add(h));
        });
        const subsystemCalls = groups.map((group: CephServiceSpec) =>
          this.nvmeofService.listSubsystems(group.spec.group).pipe(catchError(() => of([])))
        );
        const namespaceCalls = groups.map((group: CephServiceSpec) =>
          this.nvmeofService.listNamespaces(group.spec.group).pipe(catchError(() => of([])))
        );
        const gatewayGroupsDown = groups.filter(
          (g: CephServiceSpec) => (g.status?.running ?? 0) < (g.status?.size ?? 0)
        ).length;
        return forkJoin([forkJoin(subsystemCalls), forkJoin(namespaceCalls)]).pipe(
          map(([subsystemsPerGroup]: [any[], any[]]) => {
            const allSubs: NvmeofSubsystem[] = (subsystemsPerGroup as NvmeofSubsystem[][]).flat();
            const totalNamespaces = allSubs.reduce((sum, s) => sum + (s.namespace_count || 0), 0);
            const activeConnections = allSubs.reduce((s, sub) => s + (sub.initiator_count || 0), 0);
            return {
              gatewayGroups: groups.length,
              gatewayGroupsDown,
              subsystems: allSubs.length,
              namespaces: totalNamespaces,
              hosts: hostsSet.size,
              activeConnections,
              hasData: true
            } as ResourceStats;
          }),
          catchError(() =>
            of({
              gatewayGroups: groups.length,
              gatewayGroupsDown,
              subsystems: 0,
              namespaces: 0,
              hosts: hostsSet.size,
              activeConnections: 0,
              hasData: true
            } as ResourceStats)
          )
        );
      }),
      catchError(() => of(null)),
      takeUntil(this.destroy$),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  loadThroughput(): void {
    this.nvmeofThroughput$ = this.performanceCardService.getNvmeofThroughput().pipe(
      catchError(() => of({ reads: 0, writes: 0 })),
      takeUntil(this.destroy$),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  loadAlerts(): void {
    this.nvmeofAlerts$ = timer(0, ALERT_POLL_INTERVAL).pipe(
      switchMap(() => this.prometheusService.isAlertmanagerUsable()),
      switchMap((usable) => {
        if (!usable) return of([] as AlertmanagerAlert[]);
        return this.prometheusService
          .getAlerts(true)
          .pipe(catchError(() => of([] as AlertmanagerAlert[])));
      }),
      map((alerts: AlertmanagerAlert[]) => {
        const nvmeAlerts = alerts.filter(isNvmeofAlert);
        const critical = nvmeAlerts.filter(
          (a) => a.labels.severity === 'critical' && a.status.state === 'active'
        ).length;
        const warning = nvmeAlerts.filter(
          (a) => a.labels.severity === 'warning' && a.status.state === 'active'
        ).length;
        const byCategory: Record<string, number> = {};
        nvmeAlerts
          .filter((a) => a.status.state === 'active' && a.labels.category)
          .forEach((a) => {
            const cat = a.labels.category!;
            byCategory[cat] = (byCategory[cat] ?? 0) + 1;
          });
        return { critical, warning, total: critical + warning, byCategory };
      }),
      catchError(() => of({ critical: 0, warning: 0, total: 0, byCategory: {} })),
      takeUntil(this.destroy$),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${NVMEOF_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }

  readonly alertQueryParams = nvmeofAlertQueryParams;
}
