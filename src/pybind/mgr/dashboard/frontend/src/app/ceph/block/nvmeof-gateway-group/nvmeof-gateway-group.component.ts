import { Component, OnInit, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { Router } from '@angular/router';
import { BehaviorSubject, forkJoin, Observable, of, timer } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';
import { GatewayGroup, NvmeofService } from '~/app/shared/api/nvmeof.service';
import { HostService } from '~/app/shared/api/host.service';
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

@Component({
  selector: 'cd-nvmeof-gateway-group',
  templateUrl: './nvmeof-gateway-group.component.html',
  styleUrls: ['./nvmeof-gateway-group.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None,
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class NvmeofGatewayGroupComponent implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @ViewChild('dateTpl', { static: true })
  dateTpl: TemplateRef<any>;

  @ViewChild('customTableItemTemplate', { static: true })
  customTableItemTemplate: TemplateRef<any>;

  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  @ViewChild('gatewayStatusTpl', { static: true })
  gatewayStatusTpl: TemplateRef<any>;



  permission: Permission;
  tableActions: CdTableAction[];
  nodesAvailable = false;
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
    private hostService: HostService,
    public modalService: ModalCdsService,
    private cephServiceService: CephServiceService,
    public taskWrapper: TaskWrapperService,
    private notificationService: NotificationService,
    private urlBuilder: URLBuilderService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.permission = this.authStorageService.getPermissions().nvmeof;

    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        cellTemplate: this.customTableItemTemplate
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


    this.gatewayGroup$ = this.subject.pipe(
      switchMap(() =>
        this.nvmeofService.listGatewayGroups().pipe(
          switchMap((gatewayGroups: GatewayGroup[][]) => {
            const groups = gatewayGroups?.[0] ?? [];
            if (groups.length === 0) {
              return of([]);
            }
            return forkJoin(
              groups.map((group: NvmeofGatewayGroup) => {
                const isRunning = (group.status?.running ?? 0) > 0;
                const subsystemsObservable = isRunning
                  ? this.nvmeofService.listSubsystems(group.spec.group).pipe(
                      catchError(() => {
                        this.notificationService.show(
                          NotificationType.error,
                          $localize`Unable to fetch Gateway group`,
                          $localize`Gateway group does not exist`
                        );
                        return of([]);
                      })
                    )
                  : of([]);

                return subsystemsObservable.pipe(
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
                );
              })
            );
          }),
          catchError(() => {
            this.notificationService.show(
              NotificationType.error,
              $localize`Unable to fetch Gateway group`,
              $localize`Gateway group does not exist`
            );
            return of([]);
          })
        )
      )
    );
    this.checkNodesAvailability();
  }

  fetchData(): void {
    this.subject.next([]);
    this.checkNodesAvailability();
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
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
      itemDescription: $localize`gateway group`,
      bodyTemplate: this.deleteTpl,
      itemNames: [selectedGroup.spec.group],
      bodyContext: {
        disableForm,
        subsystemCount: selectedGroup.subSystemCount
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
    const groupName = selectedGroup.spec?.group ?? selectedGroup.name ?? null;
    if (!groupName) {
      return;
    }
    const url = `/block/nvmeof/gateways/view/${encodeURIComponent(groupName)}`;
    this.router.navigateByUrl(url);
  }
}
