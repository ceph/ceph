import { Component, Input, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Subscription } from 'rxjs';

import { NfsService } from '~/app/shared/api/nfs.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ViewCacheStatus } from '~/app/shared/enum/view-cache-status.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { Task } from '~/app/shared/models/task';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { getFsalFromRoute, getPathfromFsal } from '../utils';
import { SUPPORTED_FSAL } from '../models/nfs.fsal';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';

export enum RgwExportType {
  BUCKET = 'bucket',
  USER = 'user'
}

@Component({
  selector: 'cd-nfs-list',
  templateUrl: './nfs-list.component.html',
  styleUrls: ['./nfs-list.component.scss'],
  providers: [TaskListService]
})
export class NfsListComponent extends ListWithDetails implements OnInit, OnDestroy {
  @ViewChild('nfsState')
  nfsState: TemplateRef<any>;
  @ViewChild('nfsFsal', { static: true })
  nfsFsal: TemplateRef<any>;
  @ViewChild('pathTmpl', { static: true })
  pathTmpl: TemplateRef<any>;

  @ViewChild('table', { static: true })
  table: TableComponent;

  @ViewChild('protocolTpl', { static: true })
  protocolTpl: TemplateRef<any>;

  @ViewChild('transportTpl', { static: true })
  transportTpl: TemplateRef<any>;

  @Input() clusterId: string;
  modalRef: NgbModalRef;

  columns: CdTableColumn[];
  permission: Permission;
  selection = new CdTableSelection();
  summaryDataSubscription: Subscription;
  viewCacheStatus: any;
  exports: any[];
  tableActions: CdTableAction[];
  isDefaultCluster = false;
  fsal: SUPPORTED_FSAL;

  builders = {
    'nfs/create': (metadata: any) => {
      return {
        path: metadata['path'],
        cluster_id: metadata['cluster_id'],
        fsal: metadata['fsal']
      };
    }
  };

  constructor(
    private authStorageService: AuthStorageService,
    private modalService: ModalCdsService,
    private nfsService: NfsService,
    private taskListService: TaskListService,
    private taskWrapper: TaskWrapperService,
    private router: Router,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().nfs;
    this.fsal = getFsalFromRoute(this.router.url);
    const prefix = getPathfromFsal(this.fsal);
    const getNfsUri = () =>
      this.selection.first() &&
      `${encodeURI(this.selection.first().cluster_id)}/${encodeURI(
        this.selection.first().export_id
      )}`;

    const createAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => `/${prefix}/nfs/create`,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection,
      name: this.actionLabels.CREATE
    };

    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => [
        `/${prefix}/nfs/edit/${getNfsUri()}`,
        {
          rgw_export_type:
            this.fsal === SUPPORTED_FSAL.RGW && !_.isEmpty(this.selection?.first()?.path)
              ? RgwExportType.BUCKET
              : RgwExportType.USER
        }
      ],
      name: this.actionLabels.EDIT
    };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteNfsModal(),
      name: this.actionLabels.DELETE
    };

    this.tableActions = [createAction, editAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`User`,
        prop: 'fsal.user_id',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: this.fsal === SUPPORTED_FSAL.CEPH ? $localize`Path` : $localize`Bucket`,
        prop: 'path',
        flexGrow: 2,
        cellTemplate: this.pathTmpl,
        cellTransformation: CellTemplate.path
      },
      {
        name: $localize`Pseudo`,
        prop: 'pseudo',
        flexGrow: 2
      },
      {
        name: $localize`Cluster`,
        prop: 'cluster_id',
        flexGrow: 2
      },
      {
        name: $localize`Storage Backend`,
        prop: 'fsal',
        flexGrow: 2,
        cellTemplate: this.nfsFsal
      },
      {
        name: $localize`Access Type`,
        prop: 'access_type',
        flexGrow: 2
      },
      {
        name: $localize`NFS Protocol`,
        prop: 'protocols',
        flexGrow: 2,
        cellTemplate: this.protocolTpl
      },
      {
        name: $localize`Transports`,
        prop: 'transports',
        flexGrow: 2,
        cellTemplate: this.transportTpl
      }
    ];

    this.taskListService.init(
      () => this.nfsService.list(this.clusterId),
      (resp) => this.prepareResponse(resp),
      (exports) => (this.exports = exports),
      () => this.onFetchError(),
      this.taskFilter,
      this.itemFilter,
      this.builders
    );
  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }

  prepareResponse(resp: any): any[] {
    let result: any[] = [];
    const filteredresp = resp.filter((nfs: any) => nfs.fsal?.name === this.fsal);
    filteredresp.forEach((nfs: any) => {
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

  itemFilter(entry: any, task: Task) {
    return (
      entry.cluster_id === task.metadata['cluster_id'] &&
      entry.export_id === task.metadata['export_id']
    );
  }

  taskFilter(task: Task) {
    return ['nfs/create', 'nfs/delete', 'nfs/edit'].includes(task.name);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteNfsModal() {
    const cluster_id = this.selection.first().cluster_id;
    const export_id = this.selection.first().export_id;

    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: $localize`NFS export`,
      itemNames: [`${cluster_id}:${export_id}`],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('nfs/delete', {
            cluster_id: cluster_id,
            export_id: export_id
          }),
          call: this.nfsService.delete(cluster_id, export_id)
        })
    });
  }
}
