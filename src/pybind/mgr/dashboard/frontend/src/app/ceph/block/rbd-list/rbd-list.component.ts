import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { RbdService } from '../../../shared/api/rbd.service';
import {
  DeletionModalComponent
} from '../../../shared/components/deletion-modal/deletion-modal.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import {
  NotificationService
} from '../../../shared/services/notification.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskManagerMessageService } from '../../../shared/services/task-manager-message.service';
import { TaskManagerService } from '../../../shared/services/task-manager.service';
import {
  FlattenConfirmationModalComponent
} from '../flatten-confirmation-modal/flatten-confimation-modal.component';
import { RbdParentModel } from '../rbd-form/rbd-parent.model';
import { RbdModel } from './rbd-model';

@Component({
  selector: 'cd-rbd-list',
  templateUrl: './rbd-list.component.html',
  styleUrls: ['./rbd-list.component.scss']
})
export class RbdListComponent implements OnInit, OnDestroy {

  @ViewChild('usageTpl') usageTpl: TemplateRef<any>;
  @ViewChild('parentTpl') parentTpl: TemplateRef<any>;
  @ViewChild('nameTpl') nameTpl: TemplateRef<any>;

  images: any;
  executingTasks: ExecutingTask[] = [];
  columns: CdTableColumn[];
  retries: number;
  viewCacheStatusList: any[];
  selection = new CdTableSelection();

  summaryDataSubscription = null;

  modalRef: BsModalRef;

  constructor(private rbdService: RbdService,
              private dimlessBinaryPipe: DimlessBinaryPipe,
              private dimlessPipe: DimlessPipe,
              private summaryService: SummaryService,
              private modalService: BsModalService,
              private notificationService: NotificationService,
              private taskManagerMessageService: TaskManagerMessageService,
              private taskManagerService: TaskManagerService) {
  }

  ngOnInit() {
    this.columns = [
      {
        name: 'Name',
        prop: 'name',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: 'Pool',
        prop: 'pool_name',
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
        name: 'Objects',
        prop: 'num_objs',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessPipe
      },
      {
        name: 'Object size',
        prop: 'obj_size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: 'Provisioned',
        prop: 'disk_usage',
        cellClass: 'text-center',
        flexGrow: 1,
        pipe: this.dimlessBinaryPipe
      },
      {
        name: 'Total provisioned',
        prop: 'total_disk_usage',
        cellClass: 'text-center',
        flexGrow: 1,
        pipe: this.dimlessBinaryPipe
      },
      {
        name: 'Parent',
        prop: 'parent',
        flexGrow: 2,
        cellTemplate: this.parentTpl
      }
    ];

    this.summaryService.get().then(resp => {
      this.loadImages(resp.executing_tasks);
      this.summaryDataSubscription = this.summaryService.summaryData$.subscribe((data: any) => {
        this.loadImages(data.executing_tasks);
      });
    });

  }

  ngOnDestroy() {
    if (this.summaryDataSubscription) {
      this.summaryDataSubscription.unsubscribe();
    }
  }

  loadImages(executingTasks) {
    if (executingTasks === null) {
      executingTasks = this.executingTasks;
    }
    this.rbdService.list()
      .subscribe(
      (resp: any[]) => {
        let images = [];
        const viewCacheStatusMap = {};
        resp.forEach(pool => {
          if (_.isUndefined(viewCacheStatusMap[pool.status])) {
            viewCacheStatusMap[pool.status] = [];
          }
          viewCacheStatusMap[pool.status].push(pool.pool_name);
          images = images.concat(pool.value);
        });
        const viewCacheStatusList = [];
        _.forEach(viewCacheStatusMap, (value: [], key) => {
          viewCacheStatusList.push({
            status: parseInt(key, 10),
            statusFor: (value.length > 1 ? 'pools ' : 'pool ') +
            '<strong>' + value.join('</strong>, <strong>') + '</strong>'
          });
        });
        this.viewCacheStatusList = viewCacheStatusList;
        images.forEach(image => {
          image.executingTasks = this._getExecutingTasks(executingTasks,
            image.pool_name, image.name);
        });
        this.images = this.merge(images, executingTasks);
        this.executingTasks = executingTasks;
      },
      () => {
        this.viewCacheStatusList = [{status: ViewCacheStatus.ValueException}];
      }
    );
  }

  _getExecutingTasks(executingTasks: ExecutingTask[], poolName, imageName): ExecutingTask[] {
    const result: ExecutingTask[] = [];
    executingTasks.forEach(executingTask => {
      if (executingTask.name === 'rbd/snap/create' ||
        executingTask.name === 'rbd/snap/delete' ||
        executingTask.name === 'rbd/snap/edit' ||
        executingTask.name === 'rbd/snap/rollback') {
        if (poolName === executingTask.metadata['pool_name'] &&
          imageName === executingTask.metadata['image_name']) {
          result.push(executingTask);
        }
      }
    });
    return result;
  }

  private merge(rbds: RbdModel[], executingTasks: ExecutingTask[] = []) {
    const resultRBDs = _.clone(rbds);
    executingTasks.forEach((executingTask) => {
      const rbdExecuting = resultRBDs.find((rbd) => {
        return rbd.pool_name === executingTask.metadata['pool_name'] &&
          rbd.name === executingTask.metadata['image_name'];
      });
      if (rbdExecuting) {
        if (executingTask.name === 'rbd/delete') {
          rbdExecuting.cdExecuting = 'deleting';

        } else if (executingTask.name === 'rbd/edit') {
          rbdExecuting.cdExecuting = 'updating';

        } else if (executingTask.name === 'rbd/flatten') {
          rbdExecuting.cdExecuting = 'flattening';
        }

      } else if (executingTask.name === 'rbd/create') {
        const rbdModel = new RbdModel();
        rbdModel.name = executingTask.metadata['image_name'];
        rbdModel.pool_name = executingTask.metadata['pool_name'];
        rbdModel.cdExecuting = 'creating';
        resultRBDs.push(rbdModel);

      } else if (executingTask.name === 'rbd/clone') {
        const rbdModel = new RbdModel();
        rbdModel.name = executingTask.metadata['child_image_name'];
        rbdModel.pool_name = executingTask.metadata['child_pool_name'];
        rbdModel.cdExecuting = 'cloning';
        resultRBDs.push(rbdModel);

      } else if (executingTask.name === 'rbd/copy') {
        const rbdModel = new RbdModel();
        rbdModel.name = executingTask.metadata['dest_image_name'];
        rbdModel.pool_name = executingTask.metadata['dest_pool_name'];
        rbdModel.cdExecuting = 'copying';
        resultRBDs.push(rbdModel);
      }
    });
    return resultRBDs;
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteRbd(poolName: string, imageName: string) {
    const finishedTask = new FinishedTask();
    finishedTask.name = 'rbd/delete';
    finishedTask.metadata = {'pool_name': poolName, 'image_name': imageName};
    this.rbdService.delete(poolName, imageName)
      .toPromise().then((resp) => {
        if (resp.status === 202) {
          this.notificationService.show(NotificationType.info,
            `RBD deletion in progress...`,
            this.taskManagerMessageService.getDescription(finishedTask));
          const executingTask = new ExecutingTask();
          executingTask.name = finishedTask.name;
          executingTask.metadata = finishedTask.metadata;
          this.executingTasks.push(executingTask);
          this.taskManagerService.subscribe(executingTask.name, executingTask.metadata,
            (asyncFinishedTask: FinishedTask) => {
              this.notificationService.notifyTask(asyncFinishedTask);
            });
        } else {
          finishedTask.success = true;
          this.notificationService.notifyTask(finishedTask);
        }
        this.modalRef.hide();
        this.loadImages(null);
      }).catch((resp) => {
        this.modalRef.content.stopLoadingSpinner();
        finishedTask.success = false;
        finishedTask.exception = resp.error;
        this.notificationService.notifyTask(finishedTask);
      });
  }

  deleteRbdModal() {
    const poolName = this.selection.first().pool_name;
    const imageName = this.selection.first().name;
    this.modalRef = this.modalService.show(DeletionModalComponent);
    this.modalRef.content.setUp({
      metaType: 'RBD',
      pattern: `${poolName}/${imageName}`,
      deletionMethod: () => this.deleteRbd(poolName, imageName),
      modalRef: this.modalRef
    });
  }

  flattenRbd(poolName, imageName) {
    const finishedTask = new FinishedTask();
    finishedTask.name = 'rbd/flatten';
    finishedTask.metadata = {'pool_name': poolName, 'image_name': imageName};
    this.rbdService.flatten(poolName, imageName)
      .toPromise().then((resp) => {
        if (resp.status === 202) {
          this.notificationService.show(NotificationType.info,
            `RBD flatten in progress...`,
            this.taskManagerMessageService.getDescription(finishedTask));
          const executingTask = new ExecutingTask();
          executingTask.name = finishedTask.name;
          executingTask.metadata = finishedTask.metadata;
          this.executingTasks.push(executingTask);
          this.taskManagerService.subscribe(executingTask.name, executingTask.metadata,
            (asyncFinishedTask: FinishedTask) => {
              this.notificationService.notifyTask(asyncFinishedTask);
            });
        } else {
          finishedTask.success = true;
          this.notificationService.notifyTask(finishedTask);
        }
        this.modalRef.hide();
        this.loadImages(null);
      }).catch((resp) => {
        finishedTask.success = false;
        finishedTask.exception = resp.error;
        this.notificationService.notifyTask(finishedTask);
      });
  }

  flattenRbdModal() {
    const poolName = this.selection.first().pool_name;
    const imageName = this.selection.first().name;
    this.modalRef = this.modalService.show(FlattenConfirmationModalComponent);
    const parent: RbdParentModel = this.selection.first().parent;
    this.modalRef.content.parent = `${parent.pool_name}/${parent.image_name}@${parent.snap_name}`;
    this.modalRef.content.child = `${poolName}/${imageName}`;
    this.modalRef.content.onSubmit.subscribe(() => {
      this.flattenRbd(poolName, imageName);
    });
  }
}
