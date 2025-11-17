import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { Daemon } from '~/app/shared/models/daemon.interface';
import { GatewayGroup, NvmeofService } from '~/app/shared/api/nvmeof.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Icons, IconSize } from '~/app/shared/enum/icons.enum';
import { GatewaySpec } from '~/app/shared/models/service.interface';

type Gateway = {
  id: string;
  hostname: string;
  status: number;
  status_desc: string;
};

@Component({
  selector: 'cd-nvmeof-gateway-group',
  templateUrl: './nvmeof-gateway-group.component.html',
  styleUrls: ['./nvmeof-gateway-group.component.scss']
})
export class NvmeofGatewayGroupComponent implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @ViewChild('dateTpl', { static: true })
  dateTpl: TemplateRef<any>;

  @ViewChild('gatewayTpl', { static: true })
  gatewayTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  gatewayGroup$: Observable<GatewayGroup[]>;
  private subject = new BehaviorSubject<void>(undefined);
  context: CdTableFetchDataContext;
  gateways: Gateway[] = [];
  gatewayGroupName: string;
  subsystemCount: number;
  gatewayCount: number;

  icons = Icons;

  iconSize = IconSize;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private nvmeofService: NvmeofService,
    private cephServiceService: CephServiceService
  ) {}

  ngOnInit(): void {
    this.permission = this.authStorageService.getPermissions().nvmeof;

    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name'
      },
      {
        name: $localize`Gateways`,
        prop: 'gatewayCount',
        cellTemplate: this.gatewayTpl
      },
      {
        name: $localize`Target nodes`,
        prop: 'nodeCount'
      },
      {
        name: $localize`Subsystems`,
        prop: 'subSystemCount'
      },
      {
        name: $localize`Created on`,
        prop: 'created',
        cellTemplate: this.dateTpl
      }
    ];

    this.gatewayGroup$ = this.subject.pipe(
      switchMap(() =>
        this.nvmeofService.listGatewayGroups().pipe(
          switchMap((groupsResponse: GatewayGroup[][]) => {
            const groups = groupsResponse?.[0] ?? [];
            return this.cephServiceService.getDaemons(null).pipe(
              switchMap((daemons: Daemon[]) => {
                const gateways = daemons.map((d) => ({
                  id: `client.${d.daemon_name}`,
                  hostname: d.hostname,
                  status: d.status,
                  status_desc: d.status_desc
                }));
                return Promise.all(
                  groups.map(async (group: GatewaySpec) => {
                    const subs = await this.nvmeofService
                      .listSubsystems(group.spec.group)
                      .toPromise()
                      .catch(() => []);
                    return {
                      ...group,
                      name: group.spec?.group,
                      gateway: gateways.length,
                      gatewayCount: {
                        running: group.status?.running ?? 0,
                        error: (group.status?.size ?? 0) - (group.status?.running ?? 0)
                      },
                      subSystemCount: Array.isArray(subs) ? subs.length : 0,
                      nodeCount: group.placement?.hosts?.length ?? 0,
                      created: group.events
                    };
                  })
                );
              })
            );
          }),
          catchError((error) => {
            this.context?.error?.(error);
            return of([]);
          })
        )
      )
    );
  }

  fetchData(): void {
    this.subject.next();
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }
}
