import { Component, Input, OnChanges, OnInit, ViewChild } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';

import { CephServiceService } from '../../../shared/api/ceph-service.service';
import { OrchestratorService } from '../../../shared/api/orchestrator.service';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permissions } from '../../../shared/models/permissions';
import { CephService } from '../../../shared/models/service.interface';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-services',
  templateUrl: './services.component.html',
  styleUrls: ['./services.component.scss']
})
export class ServicesComponent implements OnChanges, OnInit {
  @ViewChild(TableComponent, { static: false })
  table: TableComponent;

  @Input() hostname: string;

  // Do not display these columns
  @Input() hiddenColumns: string[] = [];

  permissions: Permissions;

  checkingOrchestrator = true;
  orchestratorExist = false;
  docsUrl: string;

  columns: Array<CdTableColumn> = [];
  services: Array<CephService> = [];
  isLoadingServices = false;
  selection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private i18n: I18n,
    private orchService: OrchestratorService,
    private cephServiceService: CephServiceService,
    private summaryService: SummaryService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    const columns = [
      {
        name: this.i18n('Service'),
        prop: 'service_name',
        flexGrow: 1
      },
      {
        name: this.i18n('Container image name'),
        prop: 'container_image_name',
        flexGrow: 3
      },
      {
        name: this.i18n('Container image ID'),
        prop: 'container_image_id',
        flexGrow: 3
      },
      {
        name: this.i18n('Running'),
        prop: 'running',
        flexGrow: 1
      },
      {
        name: this.i18n('Size'),
        prop: 'size',
        flexGrow: 1
      },
      {
        name: this.i18n('Last Refreshed'),
        prop: 'last_refresh',
        flexGrow: 1
      }
    ];

    this.columns = columns.filter((col: any) => {
      return !this.hiddenColumns.includes(col.prop);
    });

    // duplicated code with grafana
    const subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }

      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/mgr/orchestrator/`;

      setTimeout(() => {
        subs.unsubscribe();
      }, 0);
    });

    this.orchService.status().subscribe((data: { available: boolean }) => {
      this.orchestratorExist = data.available;
      this.checkingOrchestrator = false;
    });
  }

  ngOnChanges() {
    if (this.orchestratorExist) {
      this.services = [];
      this.table.reloadData();
    }
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getServices(context: CdTableFetchDataContext) {
    if (this.isLoadingServices) {
      return;
    }
    this.isLoadingServices = true;
    this.cephServiceService.list().subscribe(
      (services: CephService[]) => {
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
