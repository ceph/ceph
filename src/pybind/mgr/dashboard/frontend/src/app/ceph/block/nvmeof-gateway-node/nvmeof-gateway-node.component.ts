import {
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, of, Subject, Subscription } from 'rxjs';
import { catchError, finalize, mergeMap, tap } from 'rxjs/operators';

import _ from 'lodash';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { HostStatus } from '~/app/shared/enum/host-status.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permission } from '~/app/shared/models/permissions';

import { Host } from '~/app/shared/models/host.interface';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-nvmeof-gateway-node',
  templateUrl: './nvmeof-gateway-node.component.html',
  styleUrls: ['./nvmeof-gateway-node.component.scss'],
  standalone: false
})
export class NvmeofGatewayNodeComponent implements OnInit, OnDestroy {
  @ViewChild(TableComponent, { static: true })
  table!: TableComponent;

  @ViewChild('hostNameTpl', { static: true })
  hostNameTpl!: TemplateRef<any>;

  @ViewChild('statusTpl', { static: true })
  statusTpl!: TemplateRef<any>;

  @ViewChild('addrTpl', { static: true })
  addrTpl!: TemplateRef<any>;

  @ViewChild('labelsTpl', { static: true })
  labelsTpl!: TemplateRef<any>;

  @ViewChild('deleteGatewayTpl', { static: true })
  deleteGatewayTpl: TemplateRef<any>;

  @Output() selectionChange = new EventEmitter<CdTableSelection>();
  @Output() hostsLoaded = new EventEmitter<number>();
  @Input() groupName!: string;

  usedHostnames: Set<string> = new Set();
  serviceSpec!: CephServiceSpec;

  permission: Permission;
  columns: CdTableColumn[] = [];
  hosts: Host[] = [];
  isLoadingHosts = false;
  tableActions!: CdTableAction[];

  selection = new CdTableSelection();
  icons = Icons;
  HostStatus = HostStatus;
  private tableContext: CdTableFetchDataContext = null;
  count = 5;
  orchStatus: OrchestratorStatus;
  private destroy$ = new Subject<void>();
  private sub = new Subscription();

  constructor(
    private authStorageService: AuthStorageService,
    private hostService: HostService,
    private orchService: OrchestratorService,
    private nvmeofService: NvmeofService,
    private cephServiceService: CephServiceService,
    private modalService: ModalCdsService,
    private route: ActivatedRoute,
    private taskWrapper: TaskWrapperService,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
  }

  ngOnInit(): void {
    this.route.parent.params.subscribe((params: { group: string }) => {
      this.groupName = params.group;
    });

    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.addGateway(),
        name: $localize`Add`,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeGateway(),
        name: $localize`Remove`,
        disable: (selection: CdTableSelection) => !selection.hasSelection
      }
    ];

    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 1,
        cellTemplate: this.hostNameTpl
      },
      {
        name: $localize`IP address`,
        prop: 'addr',
        flexGrow: 0.8,
        cellTemplate: this.addrTpl
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 0.8,
        cellTemplate: this.statusTpl
      },
      {
        name: $localize`Labels (tags)`,

        prop: 'labels',
        flexGrow: 1,
        cellTemplate: this.labelsTpl
      }
    ];
  }

  addGateway(): void {
    // TODO: Logic to open add gateway modal
  }

  removeGateway(): void {
    const hostname = this.selection.first().hostname;
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: 'gateway node',
      itemNames: [hostname],
      actionDescription: 'remove',
      hideDefaultWarning: true,
      impact: DeletionImpact.high,
      bodyTemplate: this.deleteGatewayTpl,
      bodyContext: {
        deletionMessage: $localize`Removing <strong>${hostname}</strong> will detach it from the gateway group and stop handling new I/O requests. Active connections may be disrupted.<br><br>You can re-add this node later if required.`
      },
      submitActionObservable: () => {
        const updatedSpec = _.cloneDeep(this.serviceSpec);
        updatedSpec.placement.hosts = updatedSpec.placement.hosts.filter((h) => h !== hostname);
        delete updatedSpec.status;
        if (updatedSpec['events']) {
          delete updatedSpec['events'];
        }
        return this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('nvmeof/gateway-node/delete', {
              hostname: hostname
            }),
            call: this.cephServiceService.update(updatedSpec)
          })
          .pipe(
            tap(() => {
              this.table.refreshBtn();
            }),
            catchError((error) => {
              this.table.refreshBtn();
              this.notificationService.show(
                NotificationType.error,
                $localize`Failed to remove gateway node ${hostname}. ${error.message}`
              );
              return of(null);
            })
          );
      }
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    if (this.sub) {
      this.sub.unsubscribe();
    }
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
    this.selectionChange.emit(selection);
  }

  getSelectedHostnames(): string[] {
    return this.selection.selected.map((host: Host) => host.hostname);
  }

  getHosts(context: CdTableFetchDataContext): void {
    if (context !== null) {
      this.tableContext = context;
    }
    if (this.tableContext == null) {
      this.tableContext = new CdTableFetchDataContext(() => undefined);
    }

    this.isLoadingHosts = true;

    if (this.sub) {
      this.sub.unsubscribe();
    }

    this.sub = this.fetchHostsAndGroups()
      .pipe(
        finalize(() => {
          this.isLoadingHosts = false;
        })
      )
      .subscribe({
        next: (result) => {
          this.processGatewayData(result.groups, result.hosts as Host[]);
        },
        error: () => {
          if (context) context.error();
        }
      });
  }

  private fetchHostsAndGroups() {
    return forkJoin({
      groups: this.nvmeofService.listGatewayGroups(),
      hosts: this.orchService.status().pipe(
        mergeMap((orchStatus) => {
          this.orchStatus = orchStatus;
          const factsAvailable = this.hostService.checkHostsFactsAvailable(orchStatus);
          return this.hostService.list(this.tableContext?.toParams(), factsAvailable.toString());
        })
      )
    });
  }

  private processGatewayData(groups: any, hostList: Host[]) {
    const groupList = groups?.[0] ?? [];

    const currentGroup = groupList.find((g: CephServiceSpec) => {
      return (
        g.service_id === `nvmeof.${this.groupName}` || g.service_id.endsWith(`.${this.groupName}`)
      );
    });

    this.serviceSpec = currentGroup;

    if (!this.serviceSpec) {
      this.hosts = [];
    } else {
      // 3. Filter Table Hosts (Current Group Only)
      const placementHosts =
        this.serviceSpec.placement?.hosts || (this.serviceSpec.spec as any)?.placement?.hosts || [];
      const currentGroupHosts = new Set<string>(placementHosts);

      this.hosts = hostList
        .map((host: Host) => ({
          ...host,
          status: host.status || HostStatus.AVAILABLE
        }))
        .filter((host: Host) => {
          return currentGroupHosts.has(host.hostname);
        });
    }

    this.count = this.hosts.length;
    this.hostsLoaded.emit(this.count);
  }
}
