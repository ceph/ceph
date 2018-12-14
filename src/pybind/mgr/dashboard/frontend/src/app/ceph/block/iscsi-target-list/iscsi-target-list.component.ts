import { Component, OnDestroy, OnInit, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { Subscription } from 'rxjs';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permissions } from '../../../shared/models/permissions';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';

@Component({
  selector: 'cd-iscsi-target-list',
  templateUrl: './iscsi-target-list.component.html',
  styleUrls: ['./iscsi-target-list.component.scss'],
  providers: [TaskListService]
})
export class IscsiTargetListComponent implements OnInit, OnDestroy {
  @ViewChild(TableComponent)
  table: TableComponent;

  available: boolean = undefined;
  columns: CdTableColumn[];
  docsUrl: string;
  modalRef: BsModalRef;
  permissions: Permissions;
  selection = new CdTableSelection();
  settings: any;
  status: string;
  summaryDataSubscription: Subscription;
  tableActions: CdTableAction[];
  targets = [];

  constructor(
    private authStorageService: AuthStorageService,
    private i18n: I18n,
    private iscsiService: IscsiService,
    private taskListService: TaskListService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private summaryservice: SummaryService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Target'),
        prop: 'target_iqn',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: this.i18n('Portals'),
        prop: 'cdPortals',
        flexGrow: 2
      },
      {
        name: this.i18n('Images'),
        prop: 'cdImages',
        flexGrow: 2
      }
    ];

    this.iscsiService.status().subscribe((result: any) => {
      this.available = result.available;

      if (result.available) {
        this.taskListService.init(
          () => this.iscsiService.listTargets(),
          (resp) => this.prepareResponse(resp),
          (targets) => (this.targets = targets),
          () => this.onFetchError(),
          () => false,
          () => false,
          undefined
        );

        this.iscsiService.settings().subscribe((settings: any) => {
          this.settings = settings;
        });
      } else {
        const summary = this.summaryservice.getCurrentSummary();
        const releaseName = this.cephReleaseNamePipe.transform(summary.version);
        this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/rbd/iscsi-targets/`;
        this.status = result.message;
      }
    });
  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }

  prepareResponse(resp: any): any[] {
    resp.forEach((element) => {
      element.cdPortals = element.portals.map((portal) => `${portal.host}:${portal.ip}`);
      element.cdImages = element.disks.map((disk) => `${disk.pool}/${disk.image}`);
    });

    return resp;
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
