import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import moment from 'moment';

import { RbdService } from '~/app/shared/api/rbd.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ViewCacheStatus } from '~/app/shared/enum/view-cache-status.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { Permission } from '~/app/shared/models/permissions';
import { Task } from '~/app/shared/models/task';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { RbdTrashPurgeModalComponent } from '../rbd-trash-purge-modal/rbd-trash-purge-modal.component';
import { RbdTrashRestoreModalComponent } from '../rbd-trash-restore-modal/rbd-trash-restore-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

@Component({
  selector: 'cd-rbd-trash-list',
  templateUrl: './rbd-trash-list.component.html',
  styleUrls: ['./rbd-trash-list.component.scss'],
  providers: [TaskListService]
})
export class RbdTrashListComponent implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  @ViewChild('expiresTpl', { static: true })
  expiresTpl: TemplateRef<any>;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  icons = Icons;

  columns: CdTableColumn[];
  executingTasks: ExecutingTask[] = [];
  images: any;
  modalRef: NgbModalRef;
  permission: Permission;
  retries: number;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  tableStatus = new TableStatusViewCache();
  disablePurgeBtn = true;

  constructor(
    private authStorageService: AuthStorageService,
    private rbdService: RbdService,
    private modalService: ModalCdsService,
    private cdDatePipe: CdDatePipe,
    public taskListService: TaskListService,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().rbdImage;
    const restoreAction: CdTableAction = {
      permission: 'update',
      icon: Icons.undo,
      click: () => this.restoreModal(),
      name: this.actionLabels.RESTORE
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteModal(),
      name: this.actionLabels.DELETE
    };
    this.tableActions = [restoreAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`ID`,
        prop: 'id',
        flexGrow: 1,
        cellTransformation: CellTemplate.executing
      },
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Pool`,
        prop: 'pool_name',
        flexGrow: 1
      },
      {
        name: $localize`Namespace`,
        prop: 'namespace',
        flexGrow: 1
      },
      {
        name: $localize`Status`,
        prop: 'deferment_end_time',
        flexGrow: 1,
        cellTemplate: this.expiresTpl
      },
      {
        name: $localize`Deleted At`,
        prop: 'deletion_time',
        flexGrow: 1,
        pipe: this.cdDatePipe
      }
    ];

    const itemFilter = (entry: any, task: Task) => {
      const imageSpec = new ImageSpec(entry.pool_name, entry.namespace, entry.id);
      return imageSpec.toString() === task.metadata['image_id_spec'];
    };

    const taskFilter = (task: Task) => {
      return ['rbd/trash/remove', 'rbd/trash/restore'].includes(task.name);
    };

    this.taskListService.init(
      () => this.rbdService.listTrash(),
      (resp) => this.prepareResponse(resp),
      (images) => (this.images = images),
      () => this.onFetchError(),
      taskFilter,
      itemFilter,
      undefined
    );
  }

  prepareResponse(resp: any[]): any[] {
    let images: any[] = [];
    const viewCacheStatusMap = {};

    resp.forEach((pool: Record<string, any>) => {
      if (_.isUndefined(viewCacheStatusMap[pool.status])) {
        viewCacheStatusMap[pool.status] = [];
      }
      viewCacheStatusMap[pool.status].push(pool.pool_name);
      images = images.concat(pool.value);
      this.disablePurgeBtn = !images.length;
    });

    let status: number;
    if (viewCacheStatusMap[3]) {
      status = 3;
    } else if (viewCacheStatusMap[1]) {
      status = 1;
    } else if (viewCacheStatusMap[2]) {
      status = 2;
    }

    if (status) {
      const statusFor =
        (viewCacheStatusMap[status].length > 1 ? 'pools ' : 'pool ') +
        viewCacheStatusMap[status].join();

      this.tableStatus = new TableStatusViewCache(status, statusFor);
    } else {
      this.tableStatus = new TableStatusViewCache();
    }

    images.forEach((image) => {
      image.cdIsExpired = moment().isAfter(image.deferment_end_time);
    });

    return images;
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
    this.tableStatus = new TableStatusViewCache(ViewCacheStatus.ValueException);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  restoreModal() {
    const initialState = {
      poolName: this.selection.first().pool_name,
      namespace: this.selection.first().namespace,
      imageName: this.selection.first().name,
      imageId: this.selection.first().id
    };

    this.modalRef = this.modalService.show(RbdTrashRestoreModalComponent, initialState);
  }

  deleteModal() {
    const poolName = this.selection.first().pool_name;
    const namespace = this.selection.first().namespace;
    const imageId = this.selection.first().id;
    const expiresAt = this.selection.first().deferment_end_time;
    const isExpired = moment().isAfter(expiresAt);
    const imageIdSpec = new ImageSpec(poolName, namespace, imageId);

    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: 'RBD',
      itemNames: [imageIdSpec],
      bodyTemplate: this.deleteTpl,
      bodyContext: { expiresAt, isExpired },
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('rbd/trash/remove', {
            image_id_spec: imageIdSpec.toString()
          }),
          call: this.rbdService.removeTrash(imageIdSpec, true)
        })
    });
  }

  purgeModal() {
    this.modalService.show(RbdTrashPurgeModalComponent);
  }
}
