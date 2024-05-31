import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { NvmeofGateway } from '~/app/shared/models/nvmeof';

import { NvmeofService } from '../nvmeof.service';

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss']
})
export class NvmeofGatewayComponent extends ListWithDetails implements OnInit {
  gateways: NvmeofGateway[] = [];
  gatewayColumns: any;
  permission: Permission;
  selection = new CdTableSelection();

  constructor(
    private nvmeofService: NvmeofService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

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

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getServiceName() {
    const service_id = this.selection.first().name.split('.')[2];
    return `nvmeof.${service_id}`;
  }

  openModal(hasCreate: boolean) {
    const BASE_URL = 'services';
    if (hasCreate) {
      this.router.navigate([
        BASE_URL,
        {
          outlets: {
            modal: [URLVerbs.CREATE, 'nvmeof']
          }
        }
      ]);
    } else {
      this.router.navigate([
        BASE_URL,
        {
          outlets: {
            modal: [URLVerbs.EDIT, 'nvmeof', this.getServiceName()]
          }
        }
      ]);
    }
  }

  getGateways() {
    this.nvmeofService.listGateways().subscribe((gateways: NvmeofGateway[] | NvmeofGateway) => {
      if (Array.isArray(gateways)) this.gateways = gateways;
      else this.gateways = [gateways];
    });
  }
}
