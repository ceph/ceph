import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ModalService } from '~/app/shared/services/modal.service';
import { MultiClusterFormComponent } from '../multi-cluster-form/multi-cluster-form.component';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permissions } from '~/app/shared/models/permissions';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { MultiCluster } from '~/app/shared/models/multi-cluster';
import { ActivatedRoute, Router } from '@angular/router';
import { CookiesService } from '~/app/shared/services/cookie.service';
import { Observable, Subscription } from 'rxjs';
import { SettingsService } from '~/app/shared/api/settings.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';

@Component({
  selector: 'cd-multi-cluster-list',
  templateUrl: './multi-cluster-list.component.html',
  styleUrls: ['./multi-cluster-list.component.scss']
})
export class MultiClusterListComponent extends ListWithDetails implements OnInit, OnDestroy {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('urlTpl', { static: true })
  public urlTpl: TemplateRef<any>;
  @ViewChild('durationTpl', { static: true })
  durationTpl: TemplateRef<any>;
  private subs = new Subscription();
  permissions: Permissions;
  tableActions: CdTableAction[];
  clusterTokenStatus: object = {};
  columns: Array<CdTableColumn> = [];
  data: any;
  selection = new CdTableSelection();
  bsModalRef: NgbModalRef;
  clustersTokenMap: Map<string, string> = new Map<string, string>();
  newData: any;
  modalRef: NgbModalRef;
  hubUrl: string;
  currentUrl: string;
  icons = Icons;
  managedByConfig$: Observable<any>;
  prometheusConnectionError: any[] = [];

  constructor(
    private multiClusterService: MultiClusterService,
    private router: Router,
    public actionLabels: ActionLabelsI18n,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private cookieService: CookiesService,
    private settingsService: SettingsService,
    private cdsModalService: ModalCdsService,
    private route: ActivatedRoute
  ) {
    super();
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        name: this.actionLabels.CONNECT,
        disable: (selection: CdTableSelection) => this.getDisable('connect', selection),
        click: () => this.openRemoteClusterInfoModal('connect')
      },
      {
        permission: 'update',
        icon: Icons.edit,
        name: this.actionLabels.EDIT,
        disable: (selection: CdTableSelection) => this.getDisable('edit', selection),
        click: () => this.openRemoteClusterInfoModal('edit')
      },
      {
        permission: 'update',
        icon: Icons.refresh,
        name: this.actionLabels.RECONNECT,
        disable: (selection: CdTableSelection) => this.getDisable('reconnect', selection),
        click: () => this.openRemoteClusterInfoModal('reconnect')
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        name: this.actionLabels.DISCONNECT,
        disable: (selection: CdTableSelection) => this.getDisable('disconnect', selection),
        click: () => this.openDeleteClusterModal()
      }
    ];
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit(): void {
    this.subs.add(
      this.multiClusterService.subscribe((resp: object) => {
        if (resp && resp['config']) {
          this.hubUrl = resp['hub_url'];
          this.currentUrl = resp['current_url'];
          const clusterDetailsArray = Object.values(resp['config']).flat();
          this.data = clusterDetailsArray;
          this.checkClusterConnectionStatus();
          this.data.forEach((cluster: any) => {
            cluster['remainingTimeWithoutSeconds'] = 0;
            if (cluster['ttl'] && cluster['ttl'] > 0) {
              cluster['ttl'] = cluster['ttl'] * 1000;
              cluster['remainingTimeWithoutSeconds'] = this.getRemainingTimeWithoutSeconds(
                cluster['ttl']
              );
              cluster['remainingDays'] = this.getRemainingDays(cluster['ttl']);
            }
          });
        }
      })
    );

    this.columns = [
      {
        prop: 'cluster_alias',
        name: $localize`Alias`,
        flexGrow: 2
      },
      {
        prop: 'cluster_connection_status',
        name: $localize`Connection`,
        flexGrow: 2,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            1: { value: 'DISCONNECTED', class: 'badge-danger' },
            0: { value: 'CONNECTED', class: 'badge-success' },
            2: { value: 'CHECKING..', class: 'badge-info' }
          }
        }
      },
      {
        prop: 'name',
        name: $localize`FSID`,
        flexGrow: 2
      },
      {
        prop: 'url',
        name: $localize`URL`,
        flexGrow: 2,
        cellTemplate: this.urlTpl
      },
      {
        prop: 'user',
        name: $localize`User`,
        flexGrow: 2
      },
      {
        prop: 'ttl',
        name: $localize`Token expires`,
        flexGrow: 2,
        cellTemplate: this.durationTpl
      }
    ];

    this.subs.add(
      this.multiClusterService.subscribeClusterTokenStatus((resp: object) => {
        this.clusterTokenStatus = resp;
        this.checkClusterConnectionStatus();
      })
    );

    this.managedByConfig$ = this.settingsService.getValues('MANAGED_BY_CLUSTERS');
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  getRemainingDays(time: number): number {
    if (time === undefined || time == null) {
      return undefined;
    }
    if (time < 0) {
      return 0;
    }
    const toDays = 1000 * 60 * 60 * 24;
    return Math.max(0, Math.floor(time / toDays));
  }

  getRemainingTimeWithoutSeconds(time: number): number {
    return Math.floor(time / (1000 * 60)) * 60 * 1000;
  }

  checkClusterConnectionStatus() {
    if (this.clusterTokenStatus && this.data) {
      this.data.forEach((cluster: MultiCluster) => {
        const clusterStatus = this.clusterTokenStatus[cluster.name];
        if (clusterStatus !== undefined) {
          cluster.cluster_connection_status = clusterStatus.status;
          cluster.ttl = clusterStatus.time_left;
        } else {
          cluster.cluster_connection_status = 2;
        }
        if (cluster.cluster_alias === 'local-cluster') {
          cluster.cluster_connection_status = 0;
        }
      });
    }
  }

  openRemoteClusterInfoModal(action: string) {
    const initialState = {
      clustersData: this.data,
      action: action,
      cluster: this.selection.first()
    };
    this.bsModalRef = this.modalService.show(MultiClusterFormComponent, initialState, {
      size: 'xl'
    });
    this.bsModalRef.componentInstance.submitAction.subscribe(() => {
      const currentRoute = this.router.url.split('?')[0];
      this.multiClusterService.refreshMultiCluster(currentRoute);
      this.checkClusterConnectionStatus();
      this.multiClusterService.isClusterAdded(true);
    });
  }

  openDeleteClusterModal() {
    const cluster = this.selection.first();
    this.modalRef = this.cdsModalService.show(CriticalConfirmationModalComponent, {
      infoMessage: $localize`Please note that the data for the disconnected cluster will be visible for a duration of ~ 5 minutes. After this period, it will be automatically removed.`,
      actionDescription: $localize`Disconnect`,
      itemDescription: $localize`Cluster`,
      itemNames: [cluster['cluster_alias'] + ' - ' + cluster['user']],
      submitAction: () =>
        this.multiClusterService.deleteCluster(cluster['name'], cluster['user']).subscribe(() => {
          this.cookieService.deleteToken(`${cluster['name']}-${cluster['user']}`);
          this.multiClusterService.showPrometheusDelayMessage(true);
          this.cdsModalService.dismissAll();
          this.notificationService.show(
            NotificationType.success,
            $localize`Disconnected cluster '${cluster['cluster_alias']}'`
          );
          const currentRoute = this.router.url.split('?')[0];
          this.multiClusterService.refreshMultiCluster(currentRoute);
        })
    });
  }

  getDisable(action: string, selection: CdTableSelection): string | boolean {
    if (this.hubUrl !== this.currentUrl) {
      return $localize`Please switch to the local-cluster to ${action} a remote cluster`;
    }
    if (!selection.hasSelection && action !== 'connect') {
      return $localize`Please select one or more clusters to ${action}`;
    }
    if (selection.hasSingleSelection) {
      const cluster = selection.first();
      if (cluster['cluster_alias'] === 'local-cluster' && action !== 'connect') {
        return $localize`Cannot ${action} local cluster`;
      }
    }
    return false;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  setExpandedRow(expandedRow: any) {
    super.setExpandedRow(expandedRow);
    this.router.navigate(['performance-details'], { relativeTo: this.route });
  }

  refresh() {
    this.multiClusterService.refresh();
    this.multiClusterService.refreshTokenStatus();
  }
}
