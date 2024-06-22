import { Component } from '@angular/core';

import { Permission } from '~/app/shared/models/permissions';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { NvmeofGateway } from '~/app/shared/models/nvmeof';

import { NvmeofService } from '../nvmeof.service';

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss']
})
export class NvmeofGatewayComponent {
  gateways: NvmeofGateway[] = [];
  gatewayColumns: any;
  permission: Permission;
  selection = new CdTableSelection();

  constructor(private nvmeofService: NvmeofService, public actionLabels: ActionLabelsI18n) {}

  ngOnInit() {
    this.gatewayColumns = [
      {
        name: $localize`Name`,
        prop: 'name'
      },
      {
        name: $localize`Address`,
        prop: 'addr'
      },
      {
        name: $localize`Port`,
        prop: 'port'
      }
    ];
  }

  getGateways() {
    this.nvmeofService.listGateways().subscribe((gateways: NvmeofGateway[] | NvmeofGateway) => {
      if (Array.isArray(gateways)) this.gateways = gateways;
      else this.gateways = [gateways];
    });
  }
}
