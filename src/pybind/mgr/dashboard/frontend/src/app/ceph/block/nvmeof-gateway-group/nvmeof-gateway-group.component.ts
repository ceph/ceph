<<<<<<< HEAD
import { Component, OnInit, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { Router } from '@angular/router';
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { GatewayGroup, NvmeofService } from '~/app/shared/api/nvmeof.service';
import { HostService } from '~/app/shared/api/host.service';
=======
import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { GatewayGroup, NvmeofService } from '~/app/shared/api/nvmeof.service';
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
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
<<<<<<< HEAD
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

const BASE_URL = 'block/nvmeof/gateways';
=======
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)

@Component({
  selector: 'cd-nvmeof-gateway-group',
  templateUrl: './nvmeof-gateway-group.component.html',
<<<<<<< HEAD
  styleUrls: ['./nvmeof-gateway-group.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None,
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
=======
  styleUrls: ['./nvmeof-gateway-group.component.scss']
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
})
export class NvmeofGatewayGroupComponent implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @ViewChild('dateTpl', { static: true })
  dateTpl: TemplateRef<any>;

<<<<<<< HEAD
  @ViewChild('customTableItemTemplate', { static: true })
  customTableItemTemplate: TemplateRef<any>;

  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

=======
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
  @ViewChild('gatewayStatusTpl', { static: true })
  gatewayStatusTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
<<<<<<< HEAD
  nodesAvailable = false;
=======
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
  columns: CdTableColumn[] = [];
  selection: CdTableSelection = new CdTableSelection();
  gatewayGroup$: Observable<CephServiceSpec[]>;
  subject = new BehaviorSubject<CephServiceSpec[]>([]);
  context: CdTableFetchDataContext;
  gatewayGroupName: string;
  subsystemCount: number;
  gatewayCount: number;

<<<<<<< HEAD
  viewUrl = `/${BASE_URL}/view`;
=======
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
  icons = Icons;

  iconSize = IconSize;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
<<<<<<< HEAD
    private nvmeofService: NvmeofService,
    private hostService: HostService,
    public modalService: ModalCdsService,
    private cephServiceService: CephServiceService,
    public taskWrapper: TaskWrapperService,
    private notificationService: NotificationService,
    private urlBuilder: URLBuilderService,
    private router: Router
=======
    private nvmeofService: NvmeofService
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
  ) {}

  ngOnInit(): void {
    this.permission = this.authStorageService.getPermissions().nvmeof;

    this.columns = [
      {
        name: $localize`Name`,
<<<<<<< HEAD
        prop: 'name',
        cellTemplate: this.customTableItemTemplate
=======
        prop: 'name'
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
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
<<<<<<< HEAD
    const createAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      disable: () => (this.nodesAvailable ? false : $localize`Gateway nodes are not available`),
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

    const viewAction: CdTableAction = {
      permission: 'read',
      icon: Icons.eye,
      click: () => this.getViewDetails(),
      name: $localize`View details`,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteGatewayGroupModal(),
      name: this.actionLabels.DELETE,
      canBePrimary: (selection: CdTableSelection) => selection.hasMultiSelection
    };

    this.tableActions = [createAction, viewAction, deleteAction];
=======
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)

    this.gatewayGroup$ = this.subject.pipe(
      switchMap(() =>
        this.nvmeofService.listGatewayGroups().pipe(
          switchMap((gatewayGroups: GatewayGroup[][]) => {
            const groups = gatewayGroups?.[0] ?? [];
<<<<<<< HEAD
            if (groups.length === 0) {
              return of([]);
            }
            return forkJoin(
              groups.map((group: NvmeofGatewayGroup) => {
                const isRunning = (group.status?.running ?? 0) > 0;
                const subsystemsObservable = isRunning
                  ? this.nvmeofService.listSubsystems(group.spec.group).pipe(
                      catchError(() => {
                        return of([]);
                      })
                    )
                  : of([]);

                return subsystemsObservable.pipe(
=======
            return forkJoin(
              groups.map((group: NvmeofGatewayGroup) =>
                this.nvmeofService.listSubsystems(group.spec.group).pipe(
                  catchError(() => of([])),
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
                  map((subs) => ({
                    ...group,
                    name: group.spec?.group,
                    statusCount: {
                      running: group.status?.running ?? 0,
                      error: (group.status?.size ?? 0) - (group.status?.running ?? 0)
                    },
<<<<<<< HEAD
=======

>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
                    subSystemCount: Array.isArray(subs) ? subs.length : 0,
                    gateWayNode: group.placement?.hosts?.length ?? 0,
                    created: group.status?.created ? new Date(group.status.created) : null
                  }))
<<<<<<< HEAD
                );
              })
            );
          }),
          catchError(() => {
=======
                )
              )
            );
          }),
          catchError((error) => {
            this.context?.error?.(error);
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
            return of([]);
          })
        )
      )
    );
<<<<<<< HEAD
    this.checkNodesAvailability();
  }
  fetchData(): void {
    this.subject.next([]);
    this.checkNodesAvailability();
=======
  }

  fetchData(): void {
    this.subject.next([]);
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }
<<<<<<< HEAD

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
      itemDescription: $localize`gateway group`,
      bodyTemplate: this.deleteTpl,
      itemNames: [selectedGroup.spec.group],
      bodyContext: {
        disableForm,
        subsystemCount: selectedGroup.subSystemCount,
        deletionMessage: $localize`Deleting <strong>${selectedGroup.spec.group}</strong> will remove all associated subsystems and may disrupt traffic routing for services relying on it. This action cannot be undone.`
      },
      submitActionObservable: () => {
        return this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('nvmeof/gateway/delete', { group: selectedGroup.spec.group }),
            call: this.cephServiceService.delete(serviceName)
          })
          .pipe(
            map(() => {
              this.table.refreshBtn();
            }),
            catchError((error) => {
              this.table.refreshBtn();
              this.notificationService.show(
                NotificationType.error,
                $localize`${`Failed to delete gateway group ${selectedGroup.spec.group}: ${error.message}`}`
              );
              return of(null);
            })
          );
      }
    });
  }

  private checkNodesAvailability(): void {
    forkJoin([this.nvmeofService.listGatewayGroups(), this.hostService.getAllHosts()]).subscribe(
      ([groups, hosts]: [GatewayGroup[][], any[]]) => {
        const usedHosts = new Set<string>();
        const groupList = groups?.[0] ?? [];
        groupList.forEach((group: CephServiceSpec) => {
          const placementHosts = group.placement?.hosts || [];
          placementHosts.forEach((hostname: string) => usedHosts.add(hostname));

          const placementLabel = group.placement?.label;
          if (placementLabel) {
            (hosts || []).forEach((host) => {
              if (host.labels?.includes(placementLabel)) {
                usedHosts.add(host.hostname);
              }
            });
          }
        });

        const availableHosts = (hosts || []).filter((host) => {
          const hostname = host.hostname;
          return hostname && !usedHosts.has(hostname);
        });

        this.nodesAvailable = availableHosts.length > 0;
      },
      () => {
        this.nodesAvailable = false;
      }
    );
  }

  getViewDetails() {
    const selectedGroup = this.selection.first();
    if (!selectedGroup) {
      return;
    }
    const groupName = selectedGroup.name;
    if (!groupName) {
      return;
    }
    this.router.navigate([this.viewUrl, groupName]);
  }
=======
>>>>>>> 0755593b4c8 ('mgr/dashboard: Carbonize Block Module > NVme-Listing Gateway group)
}
