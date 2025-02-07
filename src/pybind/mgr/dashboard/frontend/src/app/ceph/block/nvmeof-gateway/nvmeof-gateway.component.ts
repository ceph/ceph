import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';

import { NvmeofService } from '../../../shared/api/nvmeof.service';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { Daemon } from '~/app/shared/models/daemon.interface';

type ComboBoxItem = {
  content: string;
  serviceName: string;
  selected?: boolean;
};

type Gateway = {
  id: string;
  hostname: string;
  status: number;
  status_desc: string;
};

@Component({
  selector: 'cd-nvmeof-gateway',
  templateUrl: './nvmeof-gateway.component.html',
  styleUrls: ['./nvmeof-gateway.component.scss']
})
export class NvmeofGatewayComponent implements OnInit {
  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;

  gateways: Gateway[] = [];
  gatewayColumns: any;
  selection = new CdTableSelection();
  gwGroups: ComboBoxItem[] = [];
  groupService: string = null;

  constructor(
    private nvmeofService: NvmeofService,
    private cephServiceService: CephServiceService,
    public actionLabels: ActionLabelsI18n
  ) {}

  ngOnInit() {
    this.getGatewayGroups();
    this.gatewayColumns = [
      {
        name: $localize`Gateway ID`,
        prop: 'id'
      },
      {
        name: $localize`Hostname`,
        prop: 'hostname'
      },
      {
        name: $localize`Status`,
        prop: 'status_desc',
        cellTemplate: this.statusTpl
      }
    ];
  }

  // for Status column
  getStatusClass(row: Gateway): string {
    return _.get(
      {
        '-1': 'badge-danger',
        '0': 'badge-warning',
        '1': 'badge-success'
      },
      row.status,
      'badge-dark'
    );
  }

  // Gateways
  getGateways() {
    this.cephServiceService.getDaemons(this.groupService).subscribe((daemons: Daemon[]) => {
      this.gateways = daemons.length
        ? daemons.map((daemon: Daemon) => {
            return {
              id: `client.${daemon.daemon_name}`,
              hostname: daemon.hostname,
              status_desc: daemon.status_desc,
              status: daemon.status
            };
          })
        : [];
    });
  }

  // Gateway groups
  onGroupSelection(selected: ComboBoxItem) {
    selected.selected = true;
    this.groupService = selected.serviceName;
    this.getGateways();
  }

  onGroupClear() {
    this.groupService = null;
    this.getGateways();
  }

  getGatewayGroups() {
    this.nvmeofService.listGatewayGroups().subscribe((response: CephServiceSpec[][]) => {
      this.gwGroups = response?.[0]?.length
        ? response[0].map((group: CephServiceSpec) => {
            return {
              content: group?.spec?.group,
              serviceName: group?.service_name
            };
          })
        : [];
      // Select first group if no group is selected
      if (!this.groupService && this.gwGroups.length) this.onGroupSelection(this.gwGroups[0]);
    });
  }
}
