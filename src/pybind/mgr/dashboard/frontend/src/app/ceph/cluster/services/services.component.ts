import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { delay } from 'rxjs/operators';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { OrchestratorStatus } from '~/app/shared/models/orchestrator.interface';
import { Permissions } from '~/app/shared/models/permissions';
import { CephServiceSpec } from '~/app/shared/models/service.interface';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { PlacementPipe } from './placement.pipe';
import { ServiceFormComponent } from './service-form/service-form.component';

const BASE_URL = 'services';

@Component({
  selector: 'cd-services',
  templateUrl: './services.component.html',
  styleUrls: ['./services.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class ServicesComponent extends ListWithDetails implements OnChanges, OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  @ViewChild('runningTpl', { static: true })
  public runningTpl: TemplateRef<any>;

  @Input() hostname: string;

  // Do not display these columns
  @Input() hiddenColumns: string[] = [];

  @Input() hiddenServices: string[] = [];

  @Input() hasDetails = true;

  @Input() routedModal = true;

  permissions: Permissions;
  tableActions: CdTableAction[];
  showDocPanel = false;
  count = 0;
  bsModalRef: NgbModalRef;

  orchStatus: OrchestratorStatus;
  actionOrchFeatures = {
    create: [OrchestratorFeature.SERVICE_CREATE],
    update: [OrchestratorFeature.SERVICE_EDIT],
    delete: [OrchestratorFeature.SERVICE_DELETE]
  };

  columns: Array<CdTableColumn> = [];
  services: Array<CephServiceSpec> = [];
  isLoadingServices = false;
  selection: CdTableSelection = new CdTableSelection();
  icons = Icons;

  constructor(
    private actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private orchService: OrchestratorService,
    private cephServiceService: CephServiceService,
    private relativeDatePipe: RelativeDatePipe,
    private taskWrapperService: TaskWrapperService,
    private router: Router
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.openModal(),
        name: this.actionLabels.CREATE,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
        // disable: (selection: CdTableSelection) => this.getDisable('create', selection)
      },
      {
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openModal(true),
        name: this.actionLabels.EDIT,
        disable: (selection: CdTableSelection) => this.getDisable('update', selection)
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteAction(),
        name: this.actionLabels.DELETE,
        disable: (selection: CdTableSelection) => this.getDisable('delete', selection)
      }
    ];
  }

  openModal(edit = false) {
    if (this.routedModal) {
      edit
        ? this.router.navigate([
            BASE_URL,
            {
              outlets: {
                modal: [
                  URLVerbs.EDIT,
                  this.selection.first().service_type,
                  this.selection.first().service_name
                ]
              }
            }
          ])
        : this.router.navigate([BASE_URL, { outlets: { modal: [URLVerbs.CREATE] } }]);
    } else {
      let initialState = {};
      edit
        ? (initialState = {
            serviceName: this.selection.first()?.service_name,
            serviceType: this.selection?.first()?.service_type,
            hiddenServices: this.hiddenServices,
            editing: edit
          })
        : (initialState = {
            hiddenServices: this.hiddenServices,
            editing: edit
          });
      this.bsModalRef = this.modalService.show(ServiceFormComponent, initialState, { size: 'lg' });
    }
  }

  ngOnInit() {
    const columns = [
      {
        name: $localize`Service`,
        prop: 'service_name',
        flexGrow: 1
      },
      {
        name: $localize`Placement`,
        prop: '',
        pipe: new PlacementPipe(),
        flexGrow: 2
      },
      {
        name: $localize`Running`,
        prop: 'status',
        flexGrow: 1,
        cellTemplate: this.runningTpl
      },
      {
        name: $localize`Last Refreshed`,
        prop: 'status.last_refresh',
        pipe: this.relativeDatePipe,
        flexGrow: 1
      }
    ];

    this.columns = columns.filter((col: any) => {
      return !this.hiddenColumns.includes(col.prop);
    });

    this.orchService.status().subscribe((status: OrchestratorStatus) => {
      this.orchStatus = status;
      this.showDocPanel = !status.available;
    });
  }

  ngOnChanges() {
    if (this.orchStatus?.available) {
      this.services = [];
      this.table.reloadData();
    }
  }

  getDisable(
    action: 'create' | 'update' | 'delete',
    selection: CdTableSelection
  ): boolean | string {
    if (action === 'delete') {
      if (!selection?.hasSingleSelection) {
        return true;
      }
    }
    if (action === 'update') {
      const disableEditServices = ['osd', 'container'];
      if (disableEditServices.indexOf(this.selection.first()?.service_type) >= 0) {
        return true;
      }
    }
    return this.orchService.getTableActionDisableDesc(
      this.orchStatus,
      this.actionOrchFeatures[action]
    );
  }

  getServices(context: CdTableFetchDataContext) {
    if (this.isLoadingServices) {
      return;
    }
    this.isLoadingServices = true;
    const pagination_obs = this.cephServiceService.list(context.toParams());
    pagination_obs.observable.subscribe(
      (services: CephServiceSpec[]) => {
        this.services = services;
        this.count = pagination_obs.count;
        this.services = this.services.filter((col: any) => {
          return !this.hiddenServices.includes(col.service_name);
        });
        this.isLoadingServices = false;
      },
      () => {
        this.isLoadingServices = false;
        this.services = [];
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteAction() {
    const service = this.selection.first();
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: $localize`Service`,
      itemNames: [service.service_name],
      actionDescription: 'delete',
      submitActionObservable: () =>
        this.taskWrapperService
          .wrapTaskAroundCall({
            task: new FinishedTask(`service/${URLVerbs.DELETE}`, {
              service_name: service.service_name
            }),
            call: this.cephServiceService.delete(service.service_name)
          })
          .pipe(
            // Delay closing the dialog, otherwise the datatable still
            // shows the deleted service after an auto-reload.
            // Showing the dialog while delaying is done to increase
            // the user experience.
            delay(5000)
          )
    });
  }
}
