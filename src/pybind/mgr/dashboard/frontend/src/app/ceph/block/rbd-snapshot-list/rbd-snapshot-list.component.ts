import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as moment from 'moment';
import { of } from 'rxjs';

import { RbdService } from '../../../shared/api/rbd.service';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { ImageSpec } from '../../../shared/models/image-spec';
import { Permission } from '../../../shared/models/permissions';
import { Task } from '../../../shared/models/task';
import { CdDatePipe } from '../../../shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskManagerService } from '../../../shared/services/task-manager.service';
import { RbdSnapshotFormModalComponent } from '../rbd-snapshot-form/rbd-snapshot-form-modal.component';
import { RbdSnapshotActionsModel } from './rbd-snapshot-actions.model';
import { RbdSnapshotModel } from './rbd-snapshot.model';

@Component({
  selector: 'cd-rbd-snapshot-list',
  templateUrl: './rbd-snapshot-list.component.html',
  styleUrls: ['./rbd-snapshot-list.component.scss'],
  providers: [TaskListService]
})
export class RbdSnapshotListComponent implements OnInit, OnChanges {
  @Input()
  snapshots: RbdSnapshotModel[] = [];
  @Input()
  featuresName: string[];
  @Input()
  poolName: string;
  @Input()
  namespace: string;
  @Input()
  rbdName: string;
  @ViewChild('nameTpl')
  nameTpl: TemplateRef<any>;
  @ViewChild('rollbackTpl', { static: true })
  rollbackTpl: TemplateRef<any>;

  permission: Permission;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];

  data: RbdSnapshotModel[];

  columns: CdTableColumn[];

  modalRef: NgbModalRef;

  builders = {
    'rbd/snap/create': (metadata: any) => {
      const model = new RbdSnapshotModel();
      model.name = metadata['snapshot_name'];
      return model;
    }
  };

  constructor(
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private cdDatePipe: CdDatePipe,
    private rbdService: RbdService,
    private taskManagerService: TaskManagerService,
    private notificationService: NotificationService,
    private summaryService: SummaryService,
    private taskListService: TaskListService,
    private i18n: I18n,
    private actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().rbdImage;
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'name',
        cellTransformation: CellTemplate.executing,
        flexGrow: 2
      },
      {
        name: this.i18n('Size'),
        prop: 'size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: this.i18n('Provisioned'),
        prop: 'disk_usage',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: this.i18n('State'),
        prop: 'is_protected',
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            true: { value: this.i18n('PROTECTED'), class: 'badge-success' },
            false: { value: this.i18n('UNPROTECTED'), class: 'badge-info' }
          }
        }
      },
      {
        name: this.i18n('Created'),
        prop: 'timestamp',
        flexGrow: 1,
        pipe: this.cdDatePipe
      }
    ];
  }

  ngOnChanges() {
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.rbdName);

    const actions = new RbdSnapshotActionsModel(this.i18n, this.actionLabels, this.featuresName);
    actions.create.click = () => this.openCreateSnapshotModal();
    actions.rename.click = () => this.openEditSnapshotModal();
    actions.protect.click = () => this.toggleProtection();
    actions.unprotect.click = () => this.toggleProtection();
    const getImageUri = () =>
      this.selection.first() &&
      `${imageSpec.toStringEncoded()}/${encodeURIComponent(this.selection.first().name)}`;
    actions.clone.routerLink = () => `/block/rbd/clone/${getImageUri()}`;
    actions.copy.routerLink = () => `/block/rbd/copy/${getImageUri()}`;
    actions.rollback.click = () => this.rollbackModal();
    actions.deleteSnap.click = () => this.deleteSnapshotModal();
    this.tableActions = actions.ordering;

    const itemFilter = (entry: any, task: Task) => {
      return entry.name === task.metadata['snapshot_name'];
    };

    const taskFilter = (task: Task) => {
      return (
        ['rbd/snap/create', 'rbd/snap/delete', 'rbd/snap/edit', 'rbd/snap/rollback'].includes(
          task.name
        ) && imageSpec.toString() === task.metadata['image_spec']
      );
    };

    this.taskListService.init(
      () => of(this.snapshots),
      null,
      (items) => (this.data = items),
      () => (this.data = this.snapshots),
      taskFilter,
      itemFilter,
      this.builders
    );
  }

  private openSnapshotModal(taskName: string, snapName: string = null) {
    this.modalRef = this.modalService.show(RbdSnapshotFormModalComponent);
    this.modalRef.componentInstance.poolName = this.poolName;
    this.modalRef.componentInstance.imageName = this.rbdName;
    this.modalRef.componentInstance.namespace = this.namespace;
    if (snapName) {
      this.modalRef.componentInstance.setEditing();
    } else {
      // Auto-create a name for the snapshot: <image_name>_<timestamp_ISO_8601>
      // https://en.wikipedia.org/wiki/ISO_8601
      snapName = `${this.rbdName}_${moment().toISOString(true)}`;
    }
    this.modalRef.componentInstance.setSnapName(snapName);
    this.modalRef.componentInstance.onSubmit.subscribe((snapshotName: string) => {
      const executingTask = new ExecutingTask();
      executingTask.name = taskName;
      executingTask.metadata = {
        image_name: this.rbdName,
        pool_name: this.poolName,
        snapshot_name: snapshotName
      };
      this.summaryService.addRunningTask(executingTask);
      this.ngOnChanges();
    });
  }

  openCreateSnapshotModal() {
    this.openSnapshotModal('rbd/snap/create');
  }

  openEditSnapshotModal() {
    this.openSnapshotModal('rbd/snap/edit', this.selection.first().name);
  }

  toggleProtection() {
    const snapshotName = this.selection.first().name;
    const isProtected = this.selection.first().is_protected;
    const finishedTask = new FinishedTask();
    finishedTask.name = 'rbd/snap/edit';
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.rbdName);
    finishedTask.metadata = {
      image_spec: imageSpec.toString(),
      snapshot_name: snapshotName
    };
    this.rbdService
      .protectSnapshot(imageSpec, snapshotName, !isProtected)
      .toPromise()
      .then(() => {
        const executingTask = new ExecutingTask();
        executingTask.name = finishedTask.name;
        executingTask.metadata = finishedTask.metadata;
        this.summaryService.addRunningTask(executingTask);
        this.ngOnChanges();
        this.taskManagerService.subscribe(
          finishedTask.name,
          finishedTask.metadata,
          (asyncFinishedTask: FinishedTask) => {
            this.notificationService.notifyTask(asyncFinishedTask);
          }
        );
      });
  }

  _asyncTask(task: string, taskName: string, snapshotName: string) {
    const finishedTask = new FinishedTask();
    finishedTask.name = taskName;
    finishedTask.metadata = {
      image_spec: new ImageSpec(this.poolName, this.namespace, this.rbdName).toString(),
      snapshot_name: snapshotName
    };
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.rbdName);
    this.rbdService[task](imageSpec, snapshotName)
      .toPromise()
      .then(() => {
        const executingTask = new ExecutingTask();
        executingTask.name = finishedTask.name;
        executingTask.metadata = finishedTask.metadata;
        this.summaryService.addRunningTask(executingTask);
        this.modalRef.close();
        this.ngOnChanges();
        this.taskManagerService.subscribe(
          executingTask.name,
          executingTask.metadata,
          (asyncFinishedTask: FinishedTask) => {
            this.notificationService.notifyTask(asyncFinishedTask);
          }
        );
      })
      .catch(() => {
        this.modalRef.componentInstance.stopLoadingSpinner();
      });
  }

  rollbackModal() {
    const snapshotName = this.selection.selected[0].name;
    const imageSpec = new ImageSpec(this.poolName, this.namespace, this.rbdName).toString();
    const initialState = {
      titleText: this.i18n('RBD snapshot rollback'),
      buttonText: this.i18n('Rollback'),
      bodyTpl: this.rollbackTpl,
      bodyData: {
        snapName: `${imageSpec}@${snapshotName}`
      },
      onSubmit: () => {
        this._asyncTask('rollbackSnapshot', 'rbd/snap/rollback', snapshotName);
      }
    };

    this.modalRef = this.modalService.show(ConfirmationModalComponent, initialState);
  }

  deleteSnapshotModal() {
    const snapshotName = this.selection.selected[0].name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: this.i18n('RBD snapshot'),
      itemNames: [snapshotName],
      submitAction: () => this._asyncTask('deleteSnapshot', 'rbd/snap/delete', snapshotName)
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
