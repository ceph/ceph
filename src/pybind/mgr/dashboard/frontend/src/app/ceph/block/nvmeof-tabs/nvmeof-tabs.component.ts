import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Observable, Subject, forkJoin, of, timer } from 'rxjs';
import {
  catchError,
  filter,
  map,
  shareReplay,
  startWith,
  switchMap,
  takeUntil,
  tap
} from 'rxjs/operators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import {
  NvmeofThroughput,
  PerformanceCardService
} from '~/app/shared/api/performance-card.service';
import { PrometheusService } from '~/app/shared/api/prometheus.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { NvmeofSubsystem, NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';
import { isNvmeofAlert, nvmeofAlertQueryParams } from '~/app/shared/helpers/nvmeof-alert.helper';
import { NvmeofStateService } from '../nvmeof-state.service';

const NVMEOF_PATH = 'block/nvmeof';
const ALERT_POLL_INTERVAL = 30000;
const DEFAULT_THROUGHPUT: NvmeofThroughput = { reads: 0, writes: 0, combined: 0 };
const DEFAULT_ALERTS: NvmeAlerts = {
  critical: 0,
  warning: 0,
  total: 0,
  byCategory: {}
};

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

type SetupState = {
  hasGatewayGroups: boolean;
  hasSubsystems: boolean;
  hasNamespaces: boolean;
};

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
  showTabsShell = true;
  hasGatewayGroups = false;
  hasSubsystems = false;
  hasNamespaces = false;
  isAllConfigured = false;
  selectedGatewayGroup: string | null = null;
  private dismissed = false;
  private cachedResourceStats: ResourceStats | null = null;
  private cachedThroughput: NvmeofThroughput = DEFAULT_THROUGHPUT;
  private cachedAlerts: NvmeAlerts = DEFAULT_ALERTS;
  nvmeof$: Observable<ResourceStats | null> = of(null);
  nvmeofThroughput$: Observable<NvmeofThroughput> = of(DEFAULT_THROUGHPUT);
  nvmeofAlerts$: Observable<NvmeAlerts> = of(DEFAULT_ALERTS);

  private destroy$ = new Subject<void>();
  private setupStateRefresh$ = new Subject<void>();

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private nvmeofService: NvmeofService,
    private performanceCardService: PerformanceCardService,
    private prometheusService: PrometheusService,
    private nvmeofStateService: NvmeofStateService
  ) {}

  private updateActiveTab(currentPath: string): void {
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.gateways;
    this.refreshOverviewCards();
  }

  private refreshOverviewCards(): void {
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
      tap((stats) => {
        this.cachedResourceStats = stats?.hasData ? stats : null;
      }),
      startWith(this.cachedResourceStats),
      takeUntil(this.destroy$),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  loadThroughput(): void {
    this.nvmeofThroughput$ = this.performanceCardService.getNvmeofThroughput().pipe(
      catchError(() => of(DEFAULT_THROUGHPUT)),
      tap((throughput) => {
        this.cachedThroughput = throughput;
      }),
      startWith(this.cachedThroughput),
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
      catchError(() => of(DEFAULT_ALERTS)),
      tap((alerts) => {
        this.cachedAlerts = alerts;
      }),
      startWith(this.cachedAlerts),
      takeUntil(this.destroy$),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  private updateShellVisibility(currentPath: string): void {
    const urlTree = this.router.parseUrl(currentPath);
    const primarySegments =
      urlTree.root.children['primary']?.segments.map((segment) => segment.path) ?? [];
    const primaryPath = `/${primarySegments.join('/')}`;

    this.showTabsShell = /^\/block\/nvmeof\/(gateways|subsystems|namespaces)$/.test(primaryPath);
  }

  private normalizeSubsystemsResponse(response: unknown): NvmeofSubsystem[] {
    if (Array.isArray(response)) {
      return response as NvmeofSubsystem[];
    }

    const subsystems = (response as { subsystems?: NvmeofSubsystem[] } | null)?.subsystems;
    return Array.isArray(subsystems) ? subsystems : [];
  }

  private normalizeNamespacesResponse(response: unknown): NvmeofSubsystemNamespace[] {
    if (Array.isArray(response)) {
      return response as NvmeofSubsystemNamespace[];
    }

    const namespaces = (response as { namespaces?: NvmeofSubsystemNamespace[] } | null)?.namespaces;
    return Array.isArray(namespaces) ? namespaces : [];
  }

  ngOnInit(): void {
    this.updateActiveTab(this.router.url);
    this.updateShellVisibility(this.router.url);

    this.setupStateRefresh$
      .pipe(
        switchMap(() => this.fetchSetupState()),
        takeUntil(this.destroy$)
      )
      .subscribe(({ hasGatewayGroups, hasSubsystems, hasNamespaces }) => {
        this.hasGatewayGroups = hasGatewayGroups;
        this.hasSubsystems = hasSubsystems;
        this.hasNamespaces = hasNamespaces;
        this.isAllConfigured = hasGatewayGroups && hasSubsystems && hasNamespaces;
        if (!hasGatewayGroups) {
          this.dismissed = false;
        }
        this.showSetupCards = !hasGatewayGroups || !this.dismissed;
      });

    this.router.events
      .pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        takeUntil(this.destroy$)
      )
      .subscribe((event) => {
        this.updateActiveTab(event.urlAfterRedirects);
        this.updateShellVisibility(event.urlAfterRedirects);
        this.loadSetupState();
      });

    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      this.selectedGatewayGroup = params?.['group']?.trim() || null;
      this.loadSetupState();
    });

    this.nvmeofStateService.refresh$
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.loadSetupState();
        this.refreshOverviewCards();
      });
  }

  private fetchSetupState(): Observable<SetupState> {
    return this.nvmeofService.listGatewayGroups().pipe(
      switchMap((gatewayGroups: CephServiceSpec[][]) => {
        const firstItem = (gatewayGroups as any)?.[0];
        const rawGroups: CephServiceSpec[] = Array.isArray(firstItem)
          ? (firstItem as CephServiceSpec[])
          : Array.isArray(gatewayGroups)
          ? ((gatewayGroups as unknown) as CephServiceSpec[])
          : [];
        const groups = rawGroups.filter((g: CephServiceSpec) => g?.spec?.group);
        const hasGatewayGroups = groups.length > 0;

        if (!hasGatewayGroups) {
          return of({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
        }

        // Empty dropdown selection: keep setup in initial state for steps 2 and 3.
        if (!this.selectedGatewayGroup) {
          return of({ hasGatewayGroups, hasSubsystems: false, hasNamespaces: false });
        }

        const selectedGroupExists = groups.some(
          (group: CephServiceSpec) => group.spec.group === this.selectedGatewayGroup
        );
        if (!selectedGroupExists) {
          return of({ hasGatewayGroups, hasSubsystems: false, hasNamespaces: false });
        }

        return forkJoin({
          subsystemsResponse: this.nvmeofService
            .listSubsystems(this.selectedGatewayGroup)
            .pipe(catchError(() => of([]))),
          namespacesResponse: this.nvmeofService
            .listNamespaces(this.selectedGatewayGroup)
            .pipe(catchError(() => of([])))
        }).pipe(
          map(
            ({
              subsystemsResponse,
              namespacesResponse
            }: {
              subsystemsResponse: unknown;
              namespacesResponse: unknown;
            }) => {
              const subsystems = this.normalizeSubsystemsResponse(subsystemsResponse);
              const namespaces = this.normalizeNamespacesResponse(namespacesResponse);
              const totalNamespaces = subsystems.reduce(
                (sum, s) => sum + (s.namespace_count || 0),
                0
              );
              return {
                hasGatewayGroups,
                hasSubsystems: subsystems.length > 0,
                hasNamespaces: namespaces.length > 0 || totalNamespaces > 0
              };
            }
          ),
          catchError(() => of({ hasGatewayGroups, hasSubsystems: false, hasNamespaces: false }))
        );
      }),
      catchError(() => of({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false }))
    );
  }

  loadSetupState(): void {
    this.setupStateRefresh$.next();
  }

  dismissOnboarding(): void {
    this.dismissed = true;
    this.showSetupCards = false;
  }

  onSelected(tab: TABS) {
    this.activeTab = tab;
    this.router.navigate([`${NVMEOF_PATH}/${tab}`], {
      queryParamsHandling: 'preserve'
    });
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }

  readonly alertQueryParams = nvmeofAlertQueryParams;
}
