import { Component, TemplateRef, ViewChild } from '@angular/core';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

enum TABS {
  'gateways',
  'subsystem',
  'namespace'
}

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss']
})
export class NvmeofGatewayComponent {
  selectedTab: TABS;

  onSelected(tab: TABS) {
    this.selectedTab = tab;
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }

  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;
  selection = new CdTableSelection();

  constructor(public actionLabels: ActionLabelsI18n) {}
}
