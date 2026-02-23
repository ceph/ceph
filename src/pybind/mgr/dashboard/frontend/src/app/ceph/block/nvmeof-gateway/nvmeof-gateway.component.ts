import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

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
  styleUrls: ['./nvmeof-gateway.component.scss'],
  standalone: false
})
export class NvmeofGatewayComponent implements OnInit, OnDestroy {
  selectedTab: TABS;
  activeTab: TABS = TABS.gateways;

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
    this.route.queryParams.subscribe((params) => {
      if (params['tab'] && Object.values(TABS).includes(params['tab'])) {
        this.activeTab = params['tab'] as TABS;
      }
      this.breadcrumbService.setTabCrumb(TAB_LABELS[this.activeTab]);
    });
  }

  ngOnDestroy() {
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
}
