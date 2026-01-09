import { Component, OnInit, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { forkJoin } from 'rxjs';
import { map } from 'rxjs/operators';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { HostService } from '~/app/shared/api/host.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

@Component({
  selector: 'cd-nvmeof-gateway-prod',
  templateUrl: './nvmeof-gateway-prod.component.html',
  styleUrls: ['./nvmeof-gateway-prod.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class NvmeofGatewayProdComponent implements OnInit {
  @ViewChild('statusTpl', { static: true })
  statusTpl: TemplateRef<any>;

  group: string | null = null;
  serviceName: string | null = null;
  columns: CdTableColumn[] = [];
  data: any[] = [];
  selection = new CdTableSelection();
  tableActions: CdTableAction[] = [];
  permission: Permission;
  icons = Icons;

  constructor(
    private route: ActivatedRoute,
    private cephServiceService: CephServiceService,
    private hostService: HostService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n
  ) {}

  ngOnInit(): void {
    this.permission = this.authStorageService.getPermissions().nvmeof;

    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname'
      },
      {
        name: $localize`IP address`,
        prop: 'addr'
      },
      {
        name: $localize`Status`,
        prop: 'status',
        cellTemplate: this.statusTpl
      },
      {
        name: $localize`Labels (tags)`,
        prop: 'labels',
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          class: 'tag-dark'
        }
      }
    ];

    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.ADD,
      click: () => {
        // Placeholder for add logic
        console.log('Add action clicked');
      },
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

    const removeAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      name: this.actionLabels.REMOVE,
      click: () => {
        // Placeholder for remove logic
        console.log('Remove action clicked', this.selection);
      },
      disable: (selection: CdTableSelection) => !selection.hasSelection,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };

    this.tableActions = [addAction, removeAction];

    // Use current route params since this is now the routed component
    this.route.paramMap.subscribe((params) => {
      this.group = params.get('group');
      this.serviceName = params.get('service');
      if (this.serviceName) {
        this.fetchData();
      }
    });
  }

  fetchData() {
    forkJoin({
      daemons: this.cephServiceService.getDaemons(this.serviceName),
      hosts: this.hostService.getAllHosts()
    })
      .pipe(
        map(({ daemons, hosts }) => {
          const hostMap = new Map(hosts.map((h) => [h.hostname, h]));
          return daemons.map((daemon) => {
            const host = hostMap.get(daemon.hostname);
            return {
              hostname: daemon.hostname,
              addr: host?.addr || '',
              status: daemon.status === 1 ? 'Available' : 'Unavailable',
              statusRaw: daemon.status,
              labels: host?.labels || []
            };
          });
        })
      )
      .subscribe((data) => {
        this.data = data;
      });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}

