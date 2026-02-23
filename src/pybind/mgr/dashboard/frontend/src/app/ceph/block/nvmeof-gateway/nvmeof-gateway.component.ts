import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { BreadcrumbService } from '~/app/shared/services/breadcrumb.service';

enum TABS {
  gateways = 'gateways',
  subsystem = 'subsystem',
  namespace = 'namespace'
}

const TAB_LABELS: Record<TABS, string> = {
  [TABS.gateways]: $localize`Gateways`,
  [TABS.subsystem]: $localize`Subsystem`,
  [TABS.namespace]: $localize`Namespace`
};

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss']
})
export class NvmeofGatewayComponent implements OnInit, OnDestroy {
  selectedTab: TABS;
  activeTab: TABS = TABS.gateways;
  private readonly destroy$ = new Subject<void>();

  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;
  selection = new CdTableSelection();

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private router: Router,
    private breadcrumbService: BreadcrumbService
  ) {}

  ngOnInit() {
    this.route.queryParams.pipe(takeUntil(this.destroy$)).subscribe((params) => {
      if (params['tab'] && Object.values(TABS).includes(params['tab'])) {
        this.activeTab = params['tab'] as TABS;
      }
      this.breadcrumbService.setTabCrumb(TAB_LABELS[this.activeTab]);
    });

    this.router.events
      .pipe(
        filter((event) => event instanceof NavigationEnd),
        takeUntil(this.destroy$)
      )
      .subscribe(() => {
        // Run after NavigationEnd handlers so tab crumb is not cleared by global breadcrumb reset.
        setTimeout(() => this.breadcrumbService.setTabCrumb(TAB_LABELS[this.activeTab]));
      });
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
    this.breadcrumbService.clearTabCrumb();
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.activeTab = tab;
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { tab },
      queryParamsHandling: 'merge'
    });
    this.breadcrumbService.setTabCrumb(TAB_LABELS[tab]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}
