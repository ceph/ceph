import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { RbdService } from '../../../shared/api/rbd.service';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
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
  @ViewChild('flattenTpl') flattenTpl: TemplateRef<any>;

  permission: Permission;
  images: any;
  executingTasks: ExecutingTask[] = [];
  columns: CdTableColumn[];
  retries: number;
  viewCacheStatusList: any[];
  selection = new CdTableSelection();

  summaryDataSubscription = null;

  modalRef: BsModalRef;

  constructor(
    private authStorageService: AuthStorageService,
    private rbdService: RbdService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private dimlessPipe: DimlessPipe,
    private summaryService: SummaryService,
    private modalService: BsModalService,
    private taskWrapper: TaskWrapperService) {
    this.permission = this.authStorageService.getPermissions().rbdImage;
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

    this.summaryService.get().subscribe((resp: any) => {
      if (!resp) {
        return;
      }
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
    this.rbdService.list().subscribe(
      (resp: any[]) => {
        let images = [];
        const viewCacheStatusMap = {};
        resp.forEach((pool) => {
          if (_.isUndefined(viewCacheStatusMap[pool.status])) {
            viewCacheStatusMap[pool.status] = [];
          }
          viewCacheStatusMap[pool.status].push(pool.pool_name);
          images = images.concat(pool.value);
        });
        const viewCacheStatusList = [];
        _.forEach(viewCacheStatusMap, (value, key) => {
          viewCacheStatusList.push({
            status: parseInt(key, 10),
            statusFor:
              (value.length > 1 ? 'pools ' : 'pool ') +
              '<strong>' +
              value.join('</strong>, <strong>') +
              '</strong>'
          });
        });
        this.viewCacheStatusList = viewCacheStatusList;
        images.forEach((image) => {
          image.executingTasks = this._getExecutingTasks(
            executingTasks,
            image.pool_name,
            image.name
          );
        });
        this.images = this.merge(images, executingTasks);
        this.executingTasks = executingTasks;
      },
      () => {
        this.viewCacheStatusList = [{ status: ViewCacheStatus.ValueException }];
      }
    );
  }

  _getExecutingTasks(executingTasks: ExecutingTask[], poolName, imageName): ExecutingTask[] {
    const result: ExecutingTask[] = [];
    executingTasks.forEach((executingTask) => {
      if (
        executingTask.name === 'rbd/snap/create' ||
        executingTask.name === 'rbd/snap/delete' ||
        executingTask.name === 'rbd/snap/edit' ||
        executingTask.name === 'rbd/snap/rollback'
      ) {
        if (
          poolName === executingTask.metadata['pool_name'] &&
          imageName === executingTask.metadata['image_name']
        ) {
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
        return (
          rbd.pool_name === executingTask.metadata['pool_name'] &&
          rbd.name === executingTask.metadata['image_name']
        );
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
        this.pushIfNotExists(resultRBDs, rbdModel);
      } else if (executingTask.name === 'rbd/clone') {
        const rbdModel = new RbdModel();
        rbdModel.name = executingTask.metadata['child_image_name'];
        rbdModel.pool_name = executingTask.metadata['child_pool_name'];
        rbdModel.cdExecuting = 'cloning';
        this.pushIfNotExists(resultRBDs, rbdModel);
      } else if (executingTask.name === 'rbd/copy') {
        const rbdModel = new RbdModel();
        rbdModel.name = executingTask.metadata['dest_image_name'];
        rbdModel.pool_name = executingTask.metadata['dest_pool_name'];
        rbdModel.cdExecuting = 'copying';
        this.pushIfNotExists(resultRBDs, rbdModel);
      }
    });
    return resultRBDs;
  }

  private pushIfNotExists(resultRBDs: RbdModel[], rbdModel: RbdModel) {
    const exists = resultRBDs.some((resultRBD) => {
      return resultRBD.name === rbdModel.name && resultRBD.pool_name === rbdModel.pool_name;
    });
    if (!exists) {
      resultRBDs.push(rbdModel);
    }
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteRbdModal() {
    const poolName = this.selection.first().pool_name;
    const imageName = this.selection.first().name;
    this.modalRef = this.modalService.show(DeletionModalComponent);
    this.modalRef.content.setUp({
      metaType: 'RBD',
      pattern: `${poolName}/${imageName}`,
      deletionObserver: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('rbd/delete', {
            pool_name: poolName,
            image_name: imageName
          }),
          tasks: this.executingTasks,
          call: this.rbdService.delete(poolName, imageName)
        }),
      modalRef: this.modalRef
    });
  }

  flattenRbd(poolName, imageName) {
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('rbd/flatten', {
          pool_name: poolName,
          image_name: imageName
        }),
        tasks: this.executingTasks,
        call: this.rbdService.flatten(poolName, imageName)
      })
      .subscribe(undefined, undefined, () => {
        this.modalRef.hide();
        this.loadImages(null);
      });
  }

  flattenRbdModal() {
    const poolName = this.selection.first().pool_name;
    const imageName = this.selection.first().name;
    const parent: RbdParentModel = this.selection.first().parent;

    const initialState = {
      titleText: 'RBD flatten',
      buttonText: 'Flatten',
      bodyTpl: this.flattenTpl,
      bodyData: {
        parent: `${parent.pool_name}/${parent.image_name}@${parent.snap_name}`,
        child: `${poolName}/${imageName}`
      },
      onSubmit: () => {
        this.flattenRbd(poolName, imageName);
      }
    };

    this.modalRef = this.modalService.show(ConfirmationModalComponent, { initialState });
  }
}
