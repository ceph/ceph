import { Component, Input, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import _ from 'lodash';
import { Subscription } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { HostService, HostModalRef, HostFactsCapacitySource } from '~/app/shared/api/host.service';
import { HostActionService } from '~/app/shared/services/host-action.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { Host } from '~/app/shared/models/host.interface';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { ActionLabels, ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { EmptyPipe } from '~/app/shared/pipes/empty.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CdTableServerSideService } from '~/app/shared/services/cd-table-server-side.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { HostFormComponent } from './host-form/host-form.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

const BASE_URL = 'hosts';

@Component({
  selector: 'cd-hosts',
  templateUrl: './hosts.component.html',
  styleUrls: ['./hosts.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  standalone: false
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

  @Input()
  showInlineActions = true;

  permissions: Permissions;
  columns: Array<CdTableColumn> = [];
  hosts: Array<object> = [];
  isLoadingHosts = false;
  cdParams = { fromLink: '/hosts' };
  tableActions: CdTableAction[];
  expandClusterActions: CdTableAction[];
  selection = new CdTableSelection();
  modalRef?: HostModalRef;
  isExecuting = false;
  errorMessage: string[];
  enableMaintenanceBtn: boolean;
  draining: boolean = false;
  bsModalRef?: HostModalRef;

  icons = Icons;
  private tableContext: CdTableFetchDataContext = null;
  count = 5;
  viewUrl = '/hosts';

  messages = {
    nonOrchHost: $localize`The feature is disabled because the selected host is not managed by Orchestrator.`
  };

  orchStatus: OrchestratorStatus;
  actionOrchFeatures: Record<string, OrchestratorFeature[]> = {
    [ActionLabels.ADD]: [OrchestratorFeature.HOST_ADD],
    [ActionLabels.EDIT]: [
      OrchestratorFeature.HOST_LABEL_ADD,
      OrchestratorFeature.HOST_LABEL_REMOVE
    ],
    [ActionLabels.REMOVE]: [OrchestratorFeature.HOST_REMOVE],
    [ActionLabels.MAINTENANCE]: [
      OrchestratorFeature.HOST_MAINTENANCE_ENTER,
      OrchestratorFeature.HOST_MAINTENANCE_EXIT
    ],
    [ActionLabels.DRAIN]: [OrchestratorFeature.HOST_DRAIN]
  };

  constructor(
    private authStorageService: AuthStorageService,
    private emptyPipe: EmptyPipe,
    private hostService: HostService,
    private hostActionService: HostActionService,
    private actionLabels: ActionLabelsI18n,
    private router: Router,
    private orchService: OrchestratorService,
    private cdsModalService: ModalCdsService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.expandClusterActions = [
      {
        name: this.actionLabels.ADD_STORAGE,
        permission: 'create',
        buttonKind: 'secondary',
        icon: Icons.expand,
        routerLink: '/add-storage',
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
            : (this.bsModalRef = this.cdsModalService.show(HostFormComponent, {
                hideMaintenance: this.hideMaintenance
              })),
        disable: (selection: CdTableSelection) => this.getDisable(ActionLabels.ADD, selection)
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.editAction(),
        disable: (selection: CdTableSelection) => this.getDisable(ActionLabels.EDIT, selection)
      },
      {
        name: this.actionLabels.START_DRAIN,
        permission: 'update',
        icon: Icons.exit,
        click: () => this.hostDrain(),
        visible: () => !this.showGeneralActionsOnly && !this.draining
      },
      {
        name: this.actionLabels.STOP_DRAIN,
        permission: 'update',
        icon: Icons.exit,
        click: () => this.hostDrain(true),
        visible: () => !this.showGeneralActionsOnly && this.draining
      },
      {
        name: this.actionLabels.REMOVE,
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteAction(),
        disable: (selection: CdTableSelection) => this.getDisable(ActionLabels.REMOVE, selection)
      },
      {
        name: this.actionLabels.ENTER_MAINTENANCE,
        permission: 'update',
        icon: Icons.enter,
        click: () => this.hostMaintenance(),
        disable: (selection: CdTableSelection) =>
          this.getDisable(ActionLabels.MAINTENANCE, selection) ||
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
          this.getDisable(ActionLabels.MAINTENANCE, selection) ||
          this.isExecuting ||
          !this.enableMaintenanceBtn,
        visible: () => !this.showGeneralActionsOnly && this.enableMaintenanceBtn
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
        name: $localize`Labels`,
        prop: 'labels',
        flexGrow: 1,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          class: 'tag-dark'
        }
      },
      {
        name: $localize`Status`,
        prop: 'status',
        flexGrow: 0.8,
        cellTransformation: CellTemplate.tag,
        customTemplateConfig: {
          map: {
            maintenance: { class: 'tag-warning' },
            available: { class: 'tag-success' }
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
    if (this.selection.hasSelection) {
      if (this.selection.first().status === 'maintenance') {
        this.enableMaintenanceBtn = true;
      }

      this.selection.first().labels.includes('_no_schedule')
        ? (this.draining = true)
        : (this.draining = false);
    }
  }

  editAction() {
    const host = this.selection.first() as Host;
    this.hostActionService.openEditModal(host, (labels: string[]) => {
      const selectedHost = this.selection.first();
      if (selectedHost && selectedHost['hostname'] === host.hostname) {
        host.labels = labels;
        Object.assign(selectedHost, host);
      }
      // Reload the data table content.
      this.table.refreshBtn();
    });
  }

  hostMaintenance() {
    const host = this.selection.first() as Host;
    this.hostActionService.hostMaintenance(
      host,
      this.maintenanceConfirmTpl,
      (isExecuting: boolean) => (this.isExecuting = isExecuting),
      (errorMessage: string[]) => (this.errorMessage = errorMessage),
      () => this.table.refreshBtn(),
      () => undefined,
      (modalRef: HostModalRef) => {
        this.modalRef = modalRef;
      }
    );
  }

  hostDrain(stop = false) {
    const host = this.selection.first() as Host;
    this.hostActionService.hostDrain(host, stop, () => this.table.refreshBtn());
  }

  getDisable(action: string, selection: CdTableSelection): boolean | string {
    return this.hostService.getDisable(
      action,
      selection,
      this.orchStatus,
      this.actionOrchFeatures,
      this.messages.nonOrchHost,
      [ActionLabels.REMOVE, ActionLabels.EDIT, ActionLabels.MAINTENANCE, ActionLabels.DRAIN]
    );
  }

  deleteAction() {
    const hostname = this.selection.first().hostname;
    this.modalRef = this.hostActionService.deleteAction(hostname);
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
        const hostFacts = hostKey as HostFactsCapacitySource;
        const totalMemoryBytes = this.hostService.getTotalMemoryBytes(hostFacts);
        const rawCapacityBytes = this.hostService.getRawCapacityBytes(hostFacts);

        hostKey['memory_total_bytes'] = this.emptyPipe.transform(totalMemoryBytes);
        hostKey['raw_capacity'] = this.emptyPipe.transform(rawCapacityBytes);
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
