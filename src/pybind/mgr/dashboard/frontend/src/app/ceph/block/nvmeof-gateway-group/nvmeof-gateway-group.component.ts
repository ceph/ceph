import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, forkJoin, Observable, of, timer } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
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
  styleUrls: ['./nvmeof-gateway-group.component.scss'],
  standalone: false
})
export class NvmeofGatewayGroupComponent implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @ViewChild('dateTpl', { static: true })
  dateTpl: TemplateRef<any>;

  @ViewChild('gatewayStatusTpl', { static: true })
  gatewayStatusTpl: TemplateRef<any>;

  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

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
    private nvmeofService: NvmeofService,
    private router: Router,
    private route: ActivatedRoute,
    public modalService: ModalCdsService,
    private cephServiceService: CephServiceService,
    public taskWrapper: TaskWrapperService,
    private notificationService: NotificationService
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

    const createAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

      const viewAction: CdTableAction = {
        permission: 'read',
        icon: Icons.show,
        name: this.actionLabels.VIEW_DETAILS,
        // Call viewDetails so navigation uses Router with queryParams to the gateway-prod view
        click: () => this.viewDetails(this.selection.first()),
        disable: (selection: CdTableSelection) => !selection.hasSelection,
        canBePrimary: (_selection: CdTableSelection) => false
      };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteGatewayGroupModal(),
      name: this.actionLabels.DELETE,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };
    this.tableActions = [createAction, viewAction, deleteAction];
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
  viewDetails(sel: any): void {
    if (!sel) {
      return;
    }
    // Navigate to the gateway-prod component path with the group as a query param
    this.router.navigate(['gateway-prod', sel.spec?.group ?? sel.name, sel.service_name], {
      relativeTo: this.route
    });
  }

  deleteGatewayGroupModal() {
    const selectedGroup = this.selection.first();
    if (!selectedGroup) {
      return;
    }
    const {
      service_name: serviceName,
      spec: { group }
    } = selectedGroup;

    const disableForm = selectedGroup.subSystemCount > 0 || !group;

    this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemNames: [group],
      itemDescription: $localize`gateway group`,
      bodyTemplate: this.deleteTpl,
      showQuestion: false,
      bodyContext: {
        groupName: group,
        subsystemCount: selectedGroup.subSystemCount,
        disableForm,
        inputLabel: $localize`Type [resource name] to confirm`,
        inputPlaceholder: $localize`Name of resource`,
        customHeader: $localize`Delete gateway group`,
        customHeaderClass: 'cds--type-heading-compact-01'
      },
      submitActionObservable: () => {
        return this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('nvmeof/gateway/delete', { group: selectedGroup.spec.group }),
            call: this.cephServiceService.delete(serviceName)
          })
          .pipe(
            switchMap(() => timer(25000)),
            map(() => {
              this.table.refreshBtn();
            }),
            catchError((error) => {
              this.table.refreshBtn();
              this.notificationService.show(
                NotificationType.error,
                $localize`${`Failed to delete gateway group ${group}: ${error.message}`}`
              );
              return of(null);
            })
          );
      }
    });
  }
}
