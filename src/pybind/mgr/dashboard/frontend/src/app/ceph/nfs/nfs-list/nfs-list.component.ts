import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { Subscription } from 'rxjs';

import { NfsService } from '../../../shared/api/nfs.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-nfs-list',
  templateUrl: './nfs-list.component.html',
  styleUrls: ['./nfs-list.component.scss'],
  providers: [TaskListService]
})
export class NfsListComponent implements OnInit, OnDestroy {
  @ViewChild('nfsState')
  nfsState: TemplateRef<any>;
  @ViewChild('nfsFsal')
  nfsFsal: TemplateRef<any>;

  @ViewChild('table')
  table: TableComponent;

  columns: CdTableColumn[];
  permission: Permission;
  selection = new CdTableSelection();
  summaryDataSubscription: Subscription;
  viewCacheStatus: any;
  exports: any[];
  tableActions: CdTableAction[];
  isDefaultCluster = false;

  modalRef: BsModalRef;

  builders = {
    'nfs/create': (metadata) => {
      return {
        path: metadata['path'],
        cluster_id: metadata['cluster_id'],
        fsal: metadata['fsal']
      };
    }
  };

  constructor(
    private authStorageService: AuthStorageService,
    private i18n: I18n,
    private modalService: BsModalService,
    private nfsService: NfsService,
    private taskListService: TaskListService,
    private taskWrapper: TaskWrapperService
  ) {
    this.permission = this.authStorageService.getPermissions().nfs;
    const getNfsUri = () =>
      this.selection.first() &&
      `${encodeURI(this.selection.first().cluster_id)}/${encodeURI(
        this.selection.first().export_id
      )}`;

    const addAction: CdTableAction = {
      permission: 'create',
      icon: 'fa-plus',
      routerLink: () => '/nfs/add',
      canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection,
      name: this.i18n('Add')
    };

    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      routerLink: () => `/nfs/edit/${getNfsUri()}`,
      name: this.i18n('Edit')
    };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-times',
      click: () => this.deleteNfsModal(),
      name: this.i18n('Delete')
    };

    this.tableActions = [addAction, editAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Export'),
        prop: 'path',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: this.i18n('Cluster'),
        prop: 'cluster_id',
        flexGrow: 2
      },
      {
        name: this.i18n('Daemons'),
        prop: 'daemons',
        flexGrow: 2
      },
      {
        name: this.i18n('Storage Backend'),
        prop: 'fsal',
        flexGrow: 2,
        cellTemplate: this.nfsFsal
      },
      {
        name: this.i18n('Access Type'),
        prop: 'access_type',
        flexGrow: 2
      }
    ];

    this.nfsService.daemon().subscribe(
      (daemons: any) => {
        const clusters = _(daemons)
          .map((daemon) => daemon.cluster_id)
          .uniq()
          .value();

        this.isDefaultCluster = clusters.length === 1 && clusters[0] === '_default_';
        this.columns[1].isHidden = this.isDefaultCluster;
        if (this.table) {
          this.table.updateColumns();
        }

        this.taskListService.init(
          () => this.nfsService.list(),
          (resp) => this.prepareResponse(resp),
          (exports) => (this.exports = exports),
          () => this.onFetchError(),
          this.taskFilter,
          this.itemFilter,
          this.builders
        );
      },
      () => {
        this.onFetchError();
      }
    );
  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }

  prepareResponse(resp: any): any[] {
    let result = [];
    resp.forEach((nfs) => {
      nfs.id = `${nfs.cluster_id}:${nfs.export_id}`;
      nfs.state = 'LOADING';
      result = result.concat(nfs);
    });

    return result;
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
    this.viewCacheStatus = { status: ViewCacheStatus.ValueException };
  }

  itemFilter(entry, task) {
    return (
      entry.cluster_id === task.metadata['cluster_id'] &&
      entry.export_id === task.metadata['export_id']
    );
  }

  taskFilter(task) {
    return ['nfs/create', 'nfs/delete', 'nfs/edit'].includes(task.name);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteNfsModal() {
    const cluster_id = this.selection.first().cluster_id;
    const export_id = this.selection.first().export_id;

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: this.i18n('NFS'),
        submitActionObservable: () =>
          this.taskWrapper.wrapTaskAroundCall({
            task: new FinishedTask('nfs/delete', {
              cluster_id: cluster_id,
              export_id: export_id
            }),
            call: this.nfsService.delete(cluster_id, export_id)
          })
      }
    });
  }
}
