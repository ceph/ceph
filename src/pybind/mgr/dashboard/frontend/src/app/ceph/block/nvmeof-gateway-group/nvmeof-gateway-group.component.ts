import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
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
import { NvmeofGatewayGroup } from '~/app/shared/models/nvmeof';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

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

  @ViewChild('gatewayStatusTpl', { static: true })
  gatewayStatusTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  gatewayGroup$: Observable<CephServiceSpec[]>;
  subject = new BehaviorSubject<CephServiceSpec[]>([]);
  context: CdTableFetchDataContext;
  gatewayGroupName: string;
  subsystemCount: number;
  gatewayCount: number;

  icons = Icons;

  iconSize = IconSize;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private nvmeofService: NvmeofService
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
        prop: 'statusCount',
        cellTemplate: this.gatewayStatusTpl
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
          switchMap((gatewayGroups: GatewayGroup[][]) => {
            const groups = gatewayGroups?.[0] ?? [];
            return forkJoin(
              groups.map((group: NvmeofGatewayGroup) =>
                this.nvmeofService.listSubsystems(group.spec.group).pipe(
                  catchError(() => of([])),
                  map((subs) => ({
                    ...group,
                    name: group.spec?.group,
                    statusCount: {
                      running: group.status?.running ?? 0,
                      error: (group.status?.size ?? 0) - (group.status?.running ?? 0)
                    },

                    subSystemCount: Array.isArray(subs) ? subs.length : 0,
                    gateWayNode: group.placement?.hosts?.length ?? 0,
                    created: group.status?.created ? new Date(group.status.created) : null
                  }))
                )
              )
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
    this.subject.next([]);
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }
}
