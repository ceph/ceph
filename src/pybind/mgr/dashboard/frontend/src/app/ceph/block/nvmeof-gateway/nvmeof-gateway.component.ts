import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Observable, Subject, forkJoin, of, timer } from 'rxjs';
import { catchError, map, shareReplay, switchMap, takeUntil } from 'rxjs/operators';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { BreadcrumbService } from '~/app/shared/services/breadcrumb.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { NvmeofSubsystem, NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';
import { isNvmeofAlert, nvmeofAlertQueryParams } from '~/app/shared/helpers/nvmeof-alert.helper';

const ALERT_POLL_INTERVAL = 30000;

export interface NvmeAlerts {
  critical: number;
  warning: number;
  total: number;
  byCategory: Record<string, number>;
}

export interface ResourceStats {
  gatewayGroups: number;
  subsystems: number;
  namespaces: number;
  hosts: number;
  reads: number;
  writes: number;
  activeConnections: number;
  hasData: boolean;
}

enum TABS {
  gateways = 'gateways',
  subsystem = 'subsystem',
  namespace = 'namespace'
}

const TAB_LABELS: Record<TABS, string> = {
  [TABS.gateways]: $localize`Gateway groups`,
  [TABS.subsystem]: $localize`Subsystems`,
  [TABS.namespace]: $localize`Namespaces`
};

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss'],
  standalone: false
})
export class NvmeofGatewayComponent implements OnInit, OnDestroy {
  selectedTab: TABS;
  activeTab: TABS = TABS.gateways;

  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;
  selection = new CdTableSelection();
  nvmeof$: Observable<ResourceStats | null> = of(null);
  nvmeofAlerts$: Observable<NvmeAlerts> = of({
    critical: 0,
    warning: 0,
    total: 0,
    byCategory: {}
  });

  private destroy$ = new Subject<void>();

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private router: Router,
    private breadcrumbService: BreadcrumbService,
    private nvmeofService: NvmeofService,
    private prometheusService: PrometheusService
  ) {}

  ngOnInit() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      if (params['tab'] && Object.values(TABS).includes(params['tab'])) {
        this.activeTab = params['tab'] as TABS;
      }
      this.breadcrumbService.setTabCrumb(TAB_LABELS[this.activeTab]);
    });
    this.loadResourceStats();
    this.loadAlerts();
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
        const nvmeAlerts = alerts.filter((alert) => this.isNvmeofCardAlert(alert));
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

  loadResourceStats() {
    this.nvmeof$ = this.nvmeofService.listGatewayGroups().pipe(
      switchMap((gatewayGroups: CephServiceSpec[][]) => {
        const firstItem = (gatewayGroups as any)?.[0];
        const rawGroups: CephServiceSpec[] = Array.isArray(firstItem)
          ? (firstItem as CephServiceSpec[])
          : Array.isArray(gatewayGroups)
          ? ((gatewayGroups as unknown) as CephServiceSpec[])
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
        return forkJoin([forkJoin(subsystemCalls), forkJoin(namespaceCalls)]).pipe(
          map(([subsystemsPerGroup, namespacesPerGroup]: [any[], any[]]) => {
            const allSubs: NvmeofSubsystem[] = (subsystemsPerGroup as NvmeofSubsystem[][]).flat();
            const allNs: NvmeofSubsystemNamespace[] = (namespacesPerGroup as NvmeofSubsystemNamespace[][]).flat();
            const totalNamespaces = allSubs.reduce((sum, s) => sum + (s.namespace_count || 0), 0);
            const reads = allNs.reduce((s, ns) => s + (Number(ns.r_mbytes_per_second) || 0), 0);
            const writes = allNs.reduce((s, ns) => s + (Number(ns.w_mbytes_per_second) || 0), 0);
            const activeConnections = allSubs.reduce((s, sub) => s + (sub.initiator_count || 0), 0);
            return {
              gatewayGroups: groups.length,
              subsystems: allSubs.length,
              namespaces: totalNamespaces,
              hosts: hostsSet.size,
              reads,
              writes,
              activeConnections,
              hasData: true
            } as ResourceStats;
          }),
          catchError(() =>
            of({
              gatewayGroups: groups.length,
              subsystems: 0,
              namespaces: 0,
              hosts: hostsSet.size,
              reads: 0,
              writes: 0,
              activeConnections: 0,
              hasData: true
            } as ResourceStats)
          )
        );
      }),
      catchError(() => of(null)),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.breadcrumbService.clearTabCrumb();
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.activeTab = tab;
    this.breadcrumbService.setTabCrumb(TAB_LABELS[tab]);
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { tab },
      queryParamsHandling: 'merge',
      replaceUrl: true
    });
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }

  /**
   * Keep NVMe-oF card matching strict without changing shared alert filters.
   */
  private isNvmeofCardAlert(alert: AlertmanagerAlert): boolean {
    if (!isNvmeofAlert(alert)) {
      return false;
    }

    const job = alert.labels?.job?.toLowerCase();
    if (job === 'nvme' || job === 'nvmeof') {
      return true;
    }

    const alertName = alert.labels?.alertname?.toLowerCase() ?? '';
    return alertName.includes('nvme');
  }

  readonly alertQueryParams = nvmeofAlertQueryParams;
}
