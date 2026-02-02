import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

enum TABS {
  gateways = 'gateways',
  subsystem = 'subsystem',
  namespace = 'namespace'
}

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss'],
  standalone: false
})
export class NvmeofGatewayComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.gateways;

  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;
  selection = new CdTableSelection();

  constructor(public actionLabels: ActionLabelsI18n, private route: ActivatedRoute) {}

  ngOnInit() {
    this.route.queryParams.subscribe((params) => {
      if (params['tab'] && Object.values(TABS).includes(params['tab'])) {
        this.activeTab = params['tab'] as TABS;
      }
    });
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.activeTab = tab;
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}
