import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Observable, Subject, forkJoin, merge, of } from 'rxjs';
import { catchError, filter, map, switchMap, takeUntil, tap } from 'rxjs/operators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { NvmeofSubsystem, NvmeofSubsystemNamespace } from '~/app/shared/models/nvmeof';
import { NvmeofStateService } from '../nvmeof-state.service';

const NVMEOF_PATH = 'block/nvmeof';

enum TABS {
  gateways = 'gateways',
  subsystems = 'subsystems',
  namespaces = 'namespaces'
}

const TAB_ROUTES = [
  '/block/nvmeof/gateways',
  '/block/nvmeof/subsystems',
  '/block/nvmeof/namespaces'
];

type SetupState = {
  hasGatewayGroups: boolean;
  hasSubsystems: boolean;
  hasNamespaces: boolean;
};

@Component({
  selector: 'cd-nvmeof-tabs',
  templateUrl: './nvmeof-tabs.component.html',
  styleUrls: ['./nvmeof-tabs.component.scss'],
  standalone: false,
  providers: [NvmeofStateService]
})
export class NvmeofTabsComponent implements OnInit, OnDestroy {
  activeTab: TABS = TABS.gateways;
  showTabsShell = true;
  showSetupCards = false;
  hasGatewayGroups = false;
  hasSubsystems = false;
  hasNamespaces = false;
  isAllConfigured = false;
  private onboardingDismissed = false;

  private destroy$ = new Subject<void>();

  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private nvmeofService: NvmeofService,
    private nvmeofStateService: NvmeofStateService
  ) {}

  private updateActiveTab(currentPath: string): void {
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.gateways;
  }

  private updateShellVisibility(currentPath: string): void {
    const urlTree = this.router.parseUrl(currentPath);
    const primarySegments =
      urlTree.root.children['primary']?.segments.map((segment) => segment.path) ?? [];
    const primaryPath = `/${primarySegments.join('/')}`;

    this.showTabsShell = TAB_ROUTES.includes(primaryPath);
  }

  private clearOnboardingDismissed(): void {
    this.onboardingDismissed = false;
  }

  private normalizeListResponse<T>(response: unknown, key: string): T[] {
    if (Array.isArray(response)) {
      return response as T[];
    }

    const nested = (response as Record<string, T[]> | null)?.[key];
    return Array.isArray(nested) ? nested : [];
  }

  ngOnInit(): void {
    this.updateActiveTab(this.router.url);
    this.updateShellVisibility(this.router.url);

    // Merge all trigger streams to prevent memory leaks and race conditions
    merge(
      of(null), // Initial load
      this.router.events.pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        tap((event) => {
          this.updateActiveTab(event.urlAfterRedirects);
          this.updateShellVisibility(event.urlAfterRedirects);
        })
      ),
      this.route.queryParams,
      this.nvmeofStateService.refresh$
    )
      .pipe(
        switchMap(() => this.fetchSetupState()),
        takeUntil(this.destroy$)
      )
      .subscribe(({ hasGatewayGroups, hasSubsystems, hasNamespaces }) => {
        this.hasGatewayGroups = hasGatewayGroups;
        this.hasSubsystems = hasSubsystems;
        this.hasNamespaces = hasNamespaces;
        this.isAllConfigured = hasGatewayGroups && hasSubsystems && hasNamespaces;

        // Reset onboarding state if any step becomes unconfigured
        // This ensures setup cards reappear when records are deleted
        if (!hasGatewayGroups || !hasSubsystems || !hasNamespaces) {
          this.clearOnboardingDismissed();
        }

        this.showSetupCards = !this.onboardingDismissed;
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private fetchSetupState(): Observable<SetupState> {
    return this.nvmeofService.listGatewayGroups().pipe(
      switchMap((gatewayGroups: CephServiceSpec[][]) => {
        const rawGroups = Array.isArray(gatewayGroups[0]) ? gatewayGroups[0] : [];
        const groups = rawGroups.filter((serviceSpec: CephServiceSpec) => serviceSpec?.spec?.group);
        const hasGatewayGroups = groups.length > 0;

        if (!hasGatewayGroups) {
          return of({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false });
        }

        // Only check the first gateway group for existence
        const firstGroupName = groups[0].spec.group;

        return forkJoin({
          subsystems: this.nvmeofService.listSubsystems(firstGroupName).pipe(
            map((resp: unknown) => this.normalizeListResponse<NvmeofSubsystem>(resp, 'subsystems')),
            catchError(() => of([]))
          ),
          namespaces: this.nvmeofService.listNamespaces(firstGroupName).pipe(
            map((resp: unknown) =>
              this.normalizeListResponse<NvmeofSubsystemNamespace>(resp, 'namespaces')
            ),
            catchError(() => of([]))
          )
        }).pipe(
          map(({ subsystems, namespaces }) => ({
            hasGatewayGroups,
            hasSubsystems: subsystems.length > 0,
            hasNamespaces: namespaces.length > 0
          })),
          catchError(() => of({ hasGatewayGroups, hasSubsystems: false, hasNamespaces: false }))
        );
      }),
      catchError(() => of({ hasGatewayGroups: false, hasSubsystems: false, hasNamespaces: false }))
    );
  }

  dismissOnboarding(): void {
    this.onboardingDismissed = true;
    this.showSetupCards = false;
  }

  onSelected(tab: TABS) {
    this.activeTab = tab;
    this.router.navigate([NVMEOF_PATH, tab], {
      queryParamsHandling: 'preserve'
    });
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}
