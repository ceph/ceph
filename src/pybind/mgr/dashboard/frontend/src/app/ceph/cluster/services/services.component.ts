import { Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';

import { CephServiceService } from '../../../shared/api/ceph-service.service';
import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permissions } from '../../../shared/models/permissions';
import { CephServiceSpec } from '../../../shared/models/service.interface';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';

@Component({
  selector: 'cd-services',
  templateUrl: './services.component.html',
  styleUrls: ['./services.component.scss']
})
export class ServicesComponent extends ListWithDetails implements OnChanges, OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @Input() hostname: string;

  // Do not display these columns
  @Input() hiddenColumns: string[] = [];

  permissions: Permissions;

  checkingOrchestrator = true;
  hasOrchestrator = false;
  docsUrl: string;

  columns: Array<CdTableColumn> = [];
  services: Array<CephServiceSpec> = [];
  isLoadingServices = false;
  selection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private orchService: OrchestratorService,
    private cephServiceService: CephServiceService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
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
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.checkIcon
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
}
