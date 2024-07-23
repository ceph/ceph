import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  Input,
  OnChanges,
  OnInit,
  TemplateRef,
  ViewChild
} from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import moment from 'moment';
import { of } from 'rxjs';

import { RbdService } from '~/app/shared/api/rbd.service';
import { CdHelperClass } from '~/app/shared/classes/cd-helper.class';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { Permission } from '~/app/shared/models/permissions';
import { Task } from '~/app/shared/models/task';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { TaskManagerService } from '~/app/shared/services/task-manager.service';
import { RbdSnapshotFormModalComponent } from '../rbd-snapshot-form/rbd-snapshot-form-modal.component';
import { RbdSnapshotActionsModel } from './rbd-snapshot-actions.model';
import { RbdSnapshotModel } from './rbd-snapshot.model';

@Component({
  selector: 'cd-rbd-snapshot-list',
  templateUrl: './rbd-snapshot-list.component.html',
  styleUrls: ['./rbd-snapshot-list.component.scss'],
  providers: [TaskListService],
  changeDetection: ChangeDetectionStrategy.OnPush
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
  mirroring: string;
  @Input()
  primary: boolean;
  @Input()
  rbdName: string;
  @ViewChild('nameTpl')
  nameTpl: TemplateRef<any>;
  @ViewChild('rollbackTpl', { static: true })
  rollbackTpl: TemplateRef<any>;

  permission: Permission;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  rbdTableActions: RbdSnapshotActionsModel;
  imageSpec: ImageSpec;

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
    private actionLabels: ActionLabelsI18n,
    private cdr: ChangeDetectorRef
  ) {
    this.permission = this.authStorageService.getPermissions().rbdImage;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'name',
        cellTransformation: CellTemplate.executing,
        flexGrow: 2
      },
      {
        name: $localize`Size`,
        prop: 'size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: $localize`Used`,
        prop: 'disk_usage',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: $localize`State`,
        prop: 'is_protected',
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            true: { value: $localize`PROTECTED`, class: 'badge-success' },
            false: { value: $localize`UNPROTECTED`, class: 'badge-info' }
          }
        }
      },
      {
        name: $localize`Created`,
        prop: 'timestamp',
        flexGrow: 1,
        pipe: this.cdDatePipe
      }
    ];

    this.imageSpec = new ImageSpec(this.poolName, this.namespace, this.rbdName);
    this.rbdTableActions = new RbdSnapshotActionsModel(
      this.actionLabels,
      this.featuresName,
      this.rbdService
    );
    this.rbdTableActions.create.click = () => this.openCreateSnapshotModal();
    this.rbdTableActions.rename.click = () => this.openEditSnapshotModal();
    this.rbdTableActions.protect.click = () => this.toggleProtection();
    this.rbdTableActions.unprotect.click = () => this.toggleProtection();
    const getImageUri = () =>
      this.selection.first() &&
      `${this.imageSpec.toStringEncoded()}/${encodeURIComponent(this.selection.first().name)}`;
    this.rbdTableActions.clone.routerLink = () => `/block/rbd/clone/${getImageUri()}`;
    this.rbdTableActions.copy.routerLink = () => `/block/rbd/copy/${getImageUri()}`;
    this.rbdTableActions.rollback.click = () => this.rollbackModal();
    this.rbdTableActions.deleteSnap.click = () => this.deleteSnapshotModal();

    this.tableActions = this.rbdTableActions.ordering;

    const itemFilter = (entry: any, task: Task) => {
      return entry.name === task.metadata['snapshot_name'];
    };

    const taskFilter = (task: Task) => {
      return (
        ['rbd/snap/create', 'rbd/snap/delete', 'rbd/snap/edit', 'rbd/snap/rollback'].includes(
          task.name
        ) && this.imageSpec.toString() === task.metadata['image_spec']
      );
    };

    this.taskListService.init(
      () => of(this.snapshots),
      null,
      (items) => {
        const hasChanges = CdHelperClass.updateChanged(this, {
          data: items
        });
        if (hasChanges) {
          this.cdr.detectChanges();
          this.data = [...this.data];
        }
      },
      () => {
        const hasChanges = CdHelperClass.updateChanged(this, {
          data: this.snapshots
        });
        if (hasChanges) {
          this.cdr.detectChanges();
          this.data = [...this.data];
        }
      },
      taskFilter,
      itemFilter,
      this.builders
    );
  }

  ngOnChanges() {
    if (this.columns) {
      this.imageSpec = new ImageSpec(this.poolName, this.namespace, this.rbdName);
      if (this.rbdTableActions) {
        this.rbdTableActions.featuresName = this.featuresName;
      }
      this.taskListService.fetch();
    }
  }

  private openSnapshotModal(taskName: string, snapName: string = null) {
    const modalVariables = {
      mirroring: this.mirroring
    };
    this.modalRef = this.modalService.show(RbdSnapshotFormModalComponent, modalVariables);
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
        image_spec: this.imageSpec.toString(),
        snapshot_name: snapshotName
      };
      this.summaryService.addRunningTask(executingTask);
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
      titleText: $localize`RBD snapshot rollback`,
      buttonText: $localize`Rollback`,
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
      itemDescription: $localize`RBD snapshot`,
      itemNames: [snapshotName],
      submitAction: () => this._asyncTask('deleteSnapshot', 'rbd/snap/delete', snapshotName)
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
