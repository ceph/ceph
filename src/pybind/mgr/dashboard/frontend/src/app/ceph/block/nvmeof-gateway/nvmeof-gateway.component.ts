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
      const requestedTab = params['tab'];
      const activeTab = Object.values(TABS).includes(requestedTab)
        ? (requestedTab as TABS)
        : TABS.gateways;
      if (activeTab !== TABS.gateways) {
        setTimeout(() => {
          this.activeTab = activeTab;
        }, 0);
      } else {
        this.activeTab = TABS.gateways;
      }
      this.breadcrumbService.setTabCrumb(TAB_LABELS[activeTab]);
    });
  }

  ngOnDestroy() {
    this.breadcrumbService.clearTabCrumb();
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.activeTab = tab;
    this.router
      .navigate([], {
        relativeTo: this.route,
        queryParams: { tab: tab },
        queryParamsHandling: 'merge'
      })
      .then(() => this.breadcrumbService.setTabCrumb(TAB_LABELS[tab]));
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}
