import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Subscription, merge, of } from 'rxjs';
import { filter, switchMap, tap } from 'rxjs/operators';

import { NvmeofService } from '~/app/shared/api/nvmeof.service';
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
  private setupSubscription?: Subscription;

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

  ngOnInit(): void {
    this.updateActiveTab(this.router.url);
    this.updateShellVisibility(this.router.url);

    // Merge all trigger streams to prevent memory leaks and race conditions
    this.setupSubscription = merge(
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
      .pipe(switchMap(() => this.nvmeofService.fetchSetupState()))
      .subscribe(({ hasGatewayGroups, hasSubsystems, hasNamespaces }) => {
        this.hasGatewayGroups = hasGatewayGroups;
        this.hasSubsystems = hasSubsystems;
        this.hasNamespaces = hasNamespaces;
        this.isAllConfigured = hasGatewayGroups && hasSubsystems && hasNamespaces;
        this.showSetupCards = !this.isAllConfigured;
      });
  }

  ngOnDestroy(): void {
    this.setupSubscription?.unsubscribe();
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
