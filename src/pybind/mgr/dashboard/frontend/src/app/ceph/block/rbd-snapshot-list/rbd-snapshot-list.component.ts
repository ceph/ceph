import {
  Component,
  Input,
  OnChanges,
  OnInit,
  TemplateRef,
  ViewChild
} from '@angular/core';

import * as _ from 'lodash';
import { ToastsManager } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import {
  RbdService
} from '../../../shared/api/rbd.service';
import {
  DeletionModalComponent
} from '../../../shared/components/deletion-modal/deletion-modal.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { CdDatePipe } from '../../../shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import {
  NotificationService
} from '../../../shared/services/notification.service';
import { TaskManagerService } from '../../../shared/services/task-manager.service';
import { RbdSnapshotFormComponent } from '../rbd-snapshot-form/rbd-snapshot-form.component';
import {
  RollbackConfirmationModalComponent
} from '../rollback-confirmation-modal/rollback-confimation-modal.component';
import { RbdSnapshotModel } from './rbd-snapshot.model';

@Component({
  selector: 'cd-rbd-snapshot-list',
  templateUrl: './rbd-snapshot-list.component.html',
  styleUrls: ['./rbd-snapshot-list.component.scss']
})
export class RbdSnapshotListComponent implements OnInit, OnChanges {

  @Input() snapshots: RbdSnapshotModel[] = [];
  @Input() poolName: string;
  @Input() rbdName: string;
  @Input() executingTasks: ExecutingTask[] = [];

  @ViewChild('nameTpl') nameTpl: TemplateRef<any>;
  @ViewChild('protectTpl') protectTpl: TemplateRef<any>;

  data: RbdSnapshotModel[];

  columns: CdTableColumn[];

  modalRef: BsModalRef;

  selection = new CdTableSelection();

  constructor(private modalService: BsModalService,
              private dimlessBinaryPipe: DimlessBinaryPipe,
              private cdDatePipe: CdDatePipe,
              private rbdService: RbdService,
              private toastr: ToastsManager,
              private taskManagerService: TaskManagerService,
              private notificationService: NotificationService) { }

  ngOnInit() {
    this.columns = [
      {
        name: 'Name',
        prop: 'name',
        cellTransformation: CellTemplate.executing,
        flexGrow: 2
      },
      {
        name: 'Size',
        prop: 'size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: 'Provisioned',
        prop: 'disk_usage',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: 'State',
        prop: 'is_protected',
        flexGrow: 1,
        cellClass: 'text-center',
        cellTemplate: this.protectTpl
      },
      {
        name: 'Created',
        prop: 'timestamp',
        flexGrow: 1,
        pipe: this.cdDatePipe
      }
    ];
  }

  ngOnChanges() {
    this.data = this.merge(this.snapshots, this.executingTasks);
  }

  private merge(snapshots: RbdSnapshotModel[], executingTasks: ExecutingTask[] = []) {
    const resultSnapshots = _.clone(snapshots);
    executingTasks.forEach((executingTask) => {
      const snapshotExecuting = resultSnapshots.find((snapshot) => {
        return snapshot.name === executingTask.metadata['snapshot_name'];
      });
      if (snapshotExecuting) {
        if (executingTask.name === 'rbd/snap/delete') {
          snapshotExecuting.cdExecuting = 'deleting';

        } else if (executingTask.name === 'rbd/snap/edit') {
          snapshotExecuting.cdExecuting = 'updating';

        } else if (executingTask.name === 'rbd/snap/rollback') {
          snapshotExecuting.cdExecuting = 'rolling back';
        }
      } else if (executingTask.name === 'rbd/snap/create') {
        const rbdSnapshotModel = new RbdSnapshotModel();
        rbdSnapshotModel.name = executingTask.metadata['snapshot_name'];
        rbdSnapshotModel.cdExecuting = 'creating';
        resultSnapshots.push(rbdSnapshotModel);
      }
    });
    return resultSnapshots;
  }

  private openSnapshotModal(taskName: string, oldSnapshotName: string = null) {
    this.modalRef = this.modalService.show(RbdSnapshotFormComponent);
    this.modalRef.content.poolName = this.poolName;
    this.modalRef.content.imageName = this.rbdName;
    if (oldSnapshotName) {
      this.modalRef.content.setSnapName(this.selection.first().name);
    }
    this.modalRef.content.onSubmit.subscribe((snapshotName: string) => {
      const executingTask = new ExecutingTask();
      executingTask.name = taskName;
      executingTask.metadata = {'snapshot_name': snapshotName};
      this.executingTasks.push(executingTask);
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
    finishedTask.metadata = {
      'pool_name': this.poolName,
      'image_name': this.rbdName,
      'snapshot_name': snapshotName
    };
    this.rbdService.protectSnapshot(this.poolName, this.rbdName, snapshotName, !isProtected)
      .toPromise().then((resp) => {
        const executingTask = new ExecutingTask();
        executingTask.name = finishedTask.name;
        executingTask.metadata = finishedTask.metadata;
        this.executingTasks.push(executingTask);
        this.ngOnChanges();
        this.taskManagerService.subscribe(finishedTask.name, finishedTask.metadata,
          (asyncFinishedTask: FinishedTask) => {
            this.notificationService.notifyTask(asyncFinishedTask);
          });
      }).catch((resp) => {
        finishedTask.success = false;
        finishedTask.exception = resp.error;
        this.notificationService.notifyTask(finishedTask);
      });
  }

  _asyncTask(task: string, taskName: string, snapshotName: string) {
    const finishedTask = new FinishedTask();
    finishedTask.name = taskName;
    finishedTask.metadata = {
      'pool_name': this.poolName,
      'image_name': this.rbdName,
      'snapshot_name': snapshotName
    };
    this.rbdService[task](this.poolName, this.rbdName, snapshotName)
      .toPromise().then(() => {
        const executingTask = new ExecutingTask();
        executingTask.name = finishedTask.name;
        executingTask.metadata = finishedTask.metadata;
        this.executingTasks.push(executingTask);
        this.modalRef.hide();
        this.ngOnChanges();
        this.taskManagerService.subscribe(executingTask.name, executingTask.metadata,
          (asyncFinishedTask: FinishedTask) => {
            this.notificationService.notifyTask(asyncFinishedTask);
          });
      })
      .catch((resp) => {
        this.modalRef.content.stopLoadingSpinner();
        finishedTask.success = false;
        finishedTask.exception = resp.error;
        this.notificationService.notifyTask(finishedTask);
      });
  }

  rollbackModal() {
    const snapshotName = this.selection.selected[0].name;
    this.modalRef = this.modalService.show(RollbackConfirmationModalComponent);
    this.modalRef.content.snapName = `${this.poolName}/${this.rbdName}@${snapshotName}`;
    this.modalRef.content.onSubmit.subscribe((itemName: string) => {
      this._asyncTask('rollbackSnapshot', 'rbd/snap/rollback', snapshotName);
    });
  }

  deleteSnapshotModal() {
    const snapshotName = this.selection.selected[0].name;
    this.modalRef = this.modalService.show(DeletionModalComponent);
    this.modalRef.content.setUp({
      metaType: 'RBD snapshot',
      pattern: snapshotName,
      deletionMethod: () => this._asyncTask('deleteSnapshot', 'rbd/snap/delete', snapshotName),
      modalRef: this.modalRef
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
