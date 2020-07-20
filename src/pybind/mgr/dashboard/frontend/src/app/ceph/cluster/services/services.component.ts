import { Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';

import { delay, finalize } from 'rxjs/operators';

import { CephServiceService } from '../../../shared/api/ceph-service.service';
import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n, URLVerbs } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permissions } from '../../../shared/models/permissions';
import { CephServiceSpec } from '../../../shared/models/service.interface';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';

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

  @Input() hostname: string;

  // Do not display these columns
  @Input() hiddenColumns: string[] = [];

  permissions: Permissions;
  tableActions: CdTableAction[];

  checkingOrchestrator = true;
  hasOrchestrator = false;
  docsUrl: string;

  columns: Array<CdTableColumn> = [];
  services: Array<CephServiceSpec> = [];
  isLoadingServices = false;
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private orchService: OrchestratorService,
    private cephServiceService: CephServiceService,
    private taskWrapperService: TaskWrapperService,
    private urlBuilder: URLBuilderService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        routerLink: () => this.urlBuilder.getCreate(),
        name: this.actionLabels.CREATE,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteAction(),
        disable: () => !this.selection.hasSingleSelection,
        name: this.actionLabels.DELETE
      }
    ];
  }

  ngOnInit() {
    const columns = [
      {
        name: $localize`Service`,
        prop: 'service_name',
        flexGrow: 1
      },
      {
        name: $localize`Container image name`,
        prop: 'status.container_image_name',
        flexGrow: 3
      },
      {
        name: $localize`Container image ID`,
        prop: 'status.container_image_id',
        flexGrow: 3,
        cellTransformation: CellTemplate.truncate,
        customTemplateConfig: {
          length: 12
        }
      },
      {
        name: $localize`Running`,
        prop: 'status.running',
        flexGrow: 1
      },
      {
        name: $localize`Size`,
        prop: 'status.size',
        flexGrow: 1
      },
      {
        name: $localize`Last Refreshed`,
        prop: 'status.last_refresh',
        flexGrow: 1
      }
    ];

    this.columns = columns.filter((col: any) => {
      return !this.hiddenColumns.includes(col.prop);
    });

    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
    });
  }

  ngOnChanges() {
    if (this.hasOrchestrator) {
      this.services = [];
      this.table.reloadData();
    }
  }

  getServices(context: CdTableFetchDataContext) {
    if (this.isLoadingServices) {
      return;
    }
    this.isLoadingServices = true;
    this.cephServiceService.list().subscribe(
      (services: CephServiceSpec[]) => {
        this.services = services;
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
            // shows the deleted service after forcing a reload.
            // Showing the dialog while delaying is done to increase
            // the user experience.
            delay(2000),
            finalize(() => {
              // Force reloading the data table content because it is
              // auto-reloaded only every 60s.
              this.table.refreshBtn();
            })
          )
    });
  }
}
