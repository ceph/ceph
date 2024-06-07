import { Component, Input, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Subscription } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { EmptyPipe } from '~/app/shared/pipes/empty.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CdTableServerSideService } from '~/app/shared/services/cd-table-server-side.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { HostFormComponent } from './host-form/host-form.component';

const BASE_URL = 'hosts';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class HostsComponent extends ListWithDetails implements OnDestroy, OnInit {
  private sub = new Subscription();

  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('servicesTpl', { static: true })
  public servicesTpl: TemplateRef<any>;
  @ViewChild('hostMetricTmpl', { static: true })
  public hostMetricTmpl: TemplateRef<any>;
  @ViewChild('hostDimlessTmpl', { static: true })
  public hostDimlessTmpl: TemplateRef<any>;
  @ViewChild('maintenanceConfirmTpl', { static: true })
  maintenanceConfirmTpl: TemplateRef<any>;
  @ViewChild('orchTmpl', { static: true })
  orchTmpl: TemplateRef<any>;
  @ViewChild('flashTmpl', { static: true })
  flashTmpl: TemplateRef<any>;
  @ViewChild('hostNameTpl', { static: true })
  hostNameTpl: TemplateRef<any>;

  @Input()
  hiddenColumns: string[] = [];

  @Input()
  hideMaintenance = false;

  @Input()
  hasTableDetails = true;

  @Input()
  hideToolHeader = false;

  @Input()
  showGeneralActionsOnly = false;

  @Input()
  showExpandClusterBtn = true;

  permissions: Permissions;
  columns: Array<CdTableColumn> = [];
  hosts: Array<object> = [];
  isLoadingHosts = false;
  cdParams = { fromLink: '/hosts' };
  tableActions: CdTableAction[];
  expandClusterActions: CdTableAction[];
  selection = new CdTableSelection();
  modalRef: NgbModalRef;
  isExecuting = false;
  errorMessage: string;
  enableMaintenanceBtn: boolean;
  enableDrainBtn: boolean;
  bsModalRef: NgbModalRef;

  icons = Icons;
  private tableContext: CdTableFetchDataContext = null;
  count = 5;

  messages = {
    nonOrchHost: $localize`The feature is disabled because the selected host is not managed by Orchestrator.`
  };

  orchStatus: OrchestratorStatus;
  actionOrchFeatures = {
    add: [OrchestratorFeature.HOST_ADD],
    edit: [OrchestratorFeature.HOST_LABEL_ADD, OrchestratorFeature.HOST_LABEL_REMOVE],
    remove: [OrchestratorFeature.HOST_REMOVE],
    maintenance: [
      OrchestratorFeature.HOST_MAINTENANCE_ENTER,
      OrchestratorFeature.HOST_MAINTENANCE_EXIT
    ],
    drain: [OrchestratorFeature.HOST_DRAIN]
  };

  constructor(
    private authStorageService: AuthStorageService,
    private emptyPipe: EmptyPipe,
    private hostService: HostService,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    private router: Router,
    private notificationService: NotificationService,
    private orchService: OrchestratorService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.expandClusterActions = [
      {
        name: this.actionLabels.EXPAND_CLUSTER,
        permission: 'create',
        icon: Icons.expand,
        routerLink: '/expand-cluster',
        disable: (selection: CdTableSelection) => this.getDisable('add', selection),
        visible: () => this.showExpandClusterBtn
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.ADD,
        permission: 'create',
        icon: Icons.add,
        click: () =>
          this.router.url.includes('/hosts')
            ? this.router.navigate([BASE_URL, { outlets: { modal: [URLVerbs.ADD] } }])
            : (this.bsModalRef = this.modalService.show(HostFormComponent, {
                hideMaintenance: this.hideMaintenance
              })),
        disable: (selection: CdTableSelection) => this.getDisable('add', selection)
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editAction(),
        disable: (selection: CdTableSelection) => this.getDisable('edit', selection)
      },
      {
        name: this.actionLabels.START_DRAIN,
        permission: 'update',
        icon: Icons.exit,
        click: () => this.hostDrain(),
        disable: (selection: CdTableSelection) =>
          this.getDisable('drain', selection) || !this.enableDrainBtn,
        visible: () => !this.showGeneralActionsOnly && this.enableDrainBtn
      },
      {
        name: this.actionLabels.STOP_DRAIN,
        permission: 'update',
        icon: Icons.exit,
        click: () => this.hostDrain(true),
        disable: (selection: CdTableSelection) =>
          this.getDisable('drain', selection) || this.enableDrainBtn,
        visible: () => !this.showGeneralActionsOnly && !this.enableDrainBtn
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteAction(),
        disable: (selection: CdTableSelection) => this.getDisable('remove', selection)
      },
      {
        name: this.actionLabels.ENTER_MAINTENANCE,
        permission: 'update',
        icon: Icons.enter,
        click: () => this.hostMaintenance(),
        disable: (selection: CdTableSelection) =>
          this.getDisable('maintenance', selection) ||
          this.isExecuting ||
          this.enableMaintenanceBtn,
        visible: () => !this.showGeneralActionsOnly && !this.enableMaintenanceBtn
      },
      {
        name: this.actionLabels.EXIT_MAINTENANCE,
        permission: 'update',
        icon: Icons.exit,
        click: () => this.hostMaintenance(),
        disable: (selection: CdTableSelection) =>
          this.getDisable('maintenance', selection) ||
          this.isExecuting ||
          !this.enableMaintenanceBtn,
        visible: () => !this.showGeneralActionsOnly && this.enableMaintenanceBtn
      }
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Hostname`,
        prop: 'hostname',
        flexGrow: 1,
        cellTemplate: this.hostNameTpl
      },
      {
        name: $localize`Service Instances`,
        prop: 'service_instances',
        flexGrow: 1.5,
        cellTemplate: this.servicesTpl
      },
      {
        name: $localize`Labels`,
        prop: 'labels',
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-dark'
        }
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 0.8,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            maintenance: { class: 'badge-warning' },
            available: { class: 'badge-success' }
          }
        }
      },
      {
        name: $localize`Model`,
        prop: 'model',
        flexGrow: 1
      },
      {
        name: $localize`CPUs`,
        prop: 'cpu_count',
        cellTemplate: this.hostMetricTmpl,
        flexGrow: 0.3
      },
      {
        name: $localize`Cores`,
        prop: 'cpu_cores',
        cellTemplate: this.hostMetricTmpl,
        flexGrow: 0.3
      },
      {
        name: $localize`Total Memory`,
        prop: 'memory_total_bytes',
        cellTemplate: this.hostDimlessTmpl,
        flexGrow: 0.4
      },
      {
        name: $localize`Raw Capacity`,
        prop: 'raw_capacity',
        cellTemplate: this.hostDimlessTmpl,
        flexGrow: 0.5
      },
      {
        name: $localize`HDDs`,
        prop: 'hdd_count',
        cellTemplate: this.hostMetricTmpl,
        flexGrow: 0.3
      },
      {
        name: $localize`Flash`,
        prop: 'flash_count',
        headerTemplate: this.flashTmpl,
        cellTemplate: this.hostMetricTmpl,
        flexGrow: 0.3
      },
      {
        name: $localize`NICs`,
        prop: 'nic_count',
        cellTemplate: this.hostMetricTmpl,
        flexGrow: 0.3
      }
    ];

    this.columns = this.columns.filter((col: any) => {
      return !this.hiddenColumns.includes(col.prop);
    });
  }

  ngOnDestroy() {
    this.sub.unsubscribe();
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
    this.enableMaintenanceBtn = false;
    this.enableDrainBtn = false;
    if (this.selection.hasSelection) {
      if (this.selection.first().status === 'maintenance') {
        this.enableMaintenanceBtn = true;
      }

      if (!this.selection.first().labels.includes('_no_schedule')) {
        this.enableDrainBtn = true;
      }
    }
  }

  editAction() {
    this.hostService.getLabels().subscribe((resp: string[]) => {
      const host = this.selection.first();
      const labels = new Set(resp.concat(this.hostService.predefinedLabels));
      const allLabels = Array.from(labels).map((label) => {
        return { enabled: true, name: label };
      });
      this.modalService.show(FormModalComponent, {
        titleText: $localize`Edit Host: ${host.hostname}`,
        fields: [
          {
            type: 'select-badges',
            name: 'labels',
            value: host['labels'],
            label: $localize`Labels`,
            typeConfig: {
              customBadges: true,
              options: allLabels,
              messages: new SelectMessages({
                empty: $localize`There are no labels.`,
                filter: $localize`Filter or add labels`,
                add: $localize`Add label`
              })
            }
          }
        ],
        submitButtonText: $localize`Edit Host`,
        onSubmit: (values: any) => {
          this.hostService.update(host['hostname'], true, values.labels).subscribe(() => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Updated Host "${host.hostname}"`
            );
            // Reload the data table content.
            this.table.refreshBtn();
          });
        }
      });
    });
  }

  hostMaintenance() {
    this.isExecuting = true;
    const host = this.selection.first();
    if (host['status'] !== 'maintenance') {
      this.hostService.update(host['hostname'], false, [], true).subscribe(
        () => {
          this.isExecuting = false;
          this.notificationService.show(
            NotificationType.success,
            $localize`"${host.hostname}" moved to maintenance`
          );
          this.table.refreshBtn();
        },
        (error) => {
          this.isExecuting = false;
          this.errorMessage = error.error['detail'].split(/\n/);
          error.preventDefault();
          if (
            error.error['detail'].includes('WARNING') &&
            !error.error['detail'].includes('It is NOT safe to stop') &&
            !error.error['detail'].includes('ALERT') &&
            !error.error['detail'].includes('unsafe to stop')
          ) {
            const modalVariables = {
              titleText: $localize`Warning`,
              buttonText: $localize`Continue`,
              warning: true,
              bodyTpl: this.maintenanceConfirmTpl,
              showSubmit: true,
              onSubmit: () => {
                this.hostService.update(host['hostname'], false, [], true, true).subscribe(
                  () => {
                    this.modalRef.close();
                  },
                  () => this.modalRef.close()
                );
              }
            };
            this.modalRef = this.modalService.show(ConfirmationModalComponent, modalVariables);
          } else {
            this.notificationService.show(
              NotificationType.error,
              $localize`"${host.hostname}" cannot be put into maintenance`,
              $localize`${error.error['detail']}`
            );
          }
        }
      );
    } else {
      this.hostService.update(host['hostname'], false, [], true).subscribe(() => {
        this.isExecuting = false;
        this.notificationService.show(
          NotificationType.success,
          $localize`"${host.hostname}" has exited maintenance`
        );
        this.table.refreshBtn();
      });
    }
  }

  hostDrain(stop = false) {
    const host = this.selection.first();
    if (stop) {
      const index = host['labels'].indexOf('_no_schedule', 0);
      host['labels'].splice(index, 1);
      this.hostService.update(host['hostname'], true, host['labels']).subscribe(() => {
        this.notificationService.show(
          NotificationType.info,
          $localize`"${host['hostname']}" stopped draining`
        );
        this.table.refreshBtn();
      });
    } else {
      this.hostService.update(host['hostname'], false, [], false, false, true).subscribe(() => {
        this.notificationService.show(
          NotificationType.info,
          $localize`"${host['hostname']}" started draining`
        );
        this.table.refreshBtn();
      });
    }
  }

  getDisable(
    action: 'add' | 'edit' | 'remove' | 'maintenance' | 'drain',
    selection: CdTableSelection
  ): boolean | string {
    if (
      action === 'remove' ||
      action === 'edit' ||
      action === 'maintenance' ||
      action === 'drain'
    ) {
      if (!selection?.hasSingleSelection) {
        return true;
      }
      if (!_.every(selection.selected, 'sources.orchestrator')) {
        return this.messages.nonOrchHost;
      }
    }
    return this.orchService.getTableActionDisableDesc(
      this.orchStatus,
      this.actionOrchFeatures[action]
    );
  }

  deleteAction() {
    const hostname = this.selection.first().hostname;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Host',
      itemNames: [hostname],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('host/remove', { hostname: hostname }),
          call: this.hostService.delete(hostname)
        })
    });
  }

  checkHostsFactsAvailable() {
    const orchFeatures = this.orchStatus.features;
    if (!_.isEmpty(orchFeatures)) {
      if (orchFeatures.get_facts.available) {
        return true;
      }
      return false;
    }
    return false;
  }

  transformHostsData() {
    if (this.checkHostsFactsAvailable()) {
      _.forEach(this.hosts, (hostKey) => {
        hostKey['memory_total_bytes'] = this.emptyPipe.transform(hostKey['memory_total_kb'] * 1024);
        hostKey['raw_capacity'] = this.emptyPipe.transform(
          hostKey['hdd_capacity_bytes'] + hostKey['flash_capacity_bytes']
        );
      });
    } else {
      // mark host facts columns unavailable
      for (let column = 4; column < this.columns.length; column++) {
        this.columns[column]['cellTemplate'] = this.orchTmpl;
      }
    }
  }

  getHosts(context: CdTableFetchDataContext) {
    if (context !== null) {
      this.tableContext = context;
    }
    if (this.tableContext == null) {
      this.tableContext = new CdTableFetchDataContext(() => undefined);
    }
    if (this.isLoadingHosts) {
      return;
    }
    this.isLoadingHosts = true;
    this.sub = this.orchService
      .status()
      .pipe(
        mergeMap((orchStatus) => {
          this.orchStatus = orchStatus;
          const factsAvailable = this.checkHostsFactsAvailable();
          return this.hostService.list(this.tableContext?.toParams(), factsAvailable.toString());
        })
      )
      .subscribe(
        (hostList: any[]) => {
          this.hosts = hostList;
          this.hosts.forEach((host: object) => {
            if (host['status'] === '') {
              host['status'] = 'available';
            }
          });
          this.transformHostsData();
          this.isLoadingHosts = false;
          if (this.hosts.length > 0) {
            this.count = CdTableServerSideService.getCount(hostList[0]);
          } else {
            this.count = 0;
          }
        },
        () => {
          this.isLoadingHosts = false;
          context.error();
        }
      );
  }

  validValue(value: any) {
    // Check if value is a number(int or float) and that it isn't null
    return (
      Number(value) == value &&
      value % 1 == 0 &&
      value !== undefined &&
      value !== null &&
      value !== ''
    );
  }
}
