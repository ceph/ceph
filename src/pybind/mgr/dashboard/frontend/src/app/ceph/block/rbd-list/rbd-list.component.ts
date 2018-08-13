import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { RbdService } from '../../../shared/api/rbd.service';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { DeletionModalComponent } from '../../../shared/components/deletion-modal/deletion-modal.component';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { RbdParentModel } from '../rbd-form/rbd-parent.model';
import { RbdModel } from './rbd-model';

@Component({
  selector: 'cd-rbd-list',
  templateUrl: './rbd-list.component.html',
  styleUrls: ['./rbd-list.component.scss'],
  providers: [TaskListService]
})
export class RbdListComponent implements OnInit {
  @ViewChild(TableComponent)
  table: TableComponent;
  @ViewChild('usageTpl')
  usageTpl: TemplateRef<any>;
  @ViewChild('parentTpl')
  parentTpl: TemplateRef<any>;
  @ViewChild('nameTpl')
  nameTpl: TemplateRef<any>;
  @ViewChild('flattenTpl')
  flattenTpl: TemplateRef<any>;

  permission: Permission;
  images: any;
  columns: CdTableColumn[];
  retries: number;
  viewCacheStatusList: any[];
  selection = new CdTableSelection();

  modalRef: BsModalRef;

  builders = {
    'rbd/create': (metadata) =>
      this.createRbdFromTask(metadata['pool_name'], metadata['image_name']),
    'rbd/clone': (metadata) =>
      this.createRbdFromTask(metadata['child_pool_name'], metadata['child_image_name']),
    'rbd/copy': (metadata) =>
      this.createRbdFromTask(metadata['dest_pool_name'], metadata['dest_image_name'])
  };

  private createRbdFromTask(pool: string, name: string): RbdModel {
    const model = new RbdModel();
    model.id = '-1';
    model.name = name;
    model.pool_name = pool;
    return model;
  }

  constructor(
    private authStorageService: AuthStorageService,
    private rbdService: RbdService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private dimlessPipe: DimlessPipe,
    private modalService: BsModalService,
    private taskWrapper: TaskWrapperService,
    private taskListService: TaskListService
  ) {
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

    this.taskListService.init(
      () => this.rbdService.list(),
      (resp) => this.prepareResponse(resp),
      (images) => (this.images = images),
      () => this.onFetchError(),
      this.taskFilter,
      this.itemFilter,
      this.builders
    );
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
    this.viewCacheStatusList = [{ status: ViewCacheStatus.ValueException }];
  }

  prepareResponse(resp: any[]): any[] {
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
    _.forEach(viewCacheStatusMap, (value: any, key) => {
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
    return images;
  }

  itemFilter(entry, task) {
    return (
      entry.pool_name === task.metadata['pool_name'] && entry.name === task.metadata['image_name']
    );
  }

  taskFilter(task) {
    return [
      'rbd/clone',
      'rbd/copy',
      'rbd/create',
      'rbd/delete',
      'rbd/edit',
      'rbd/flatten'
    ].includes(task.name);
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
        call: this.rbdService.flatten(poolName, imageName)
      })
      .subscribe(undefined, undefined, () => {
        this.modalRef.hide();
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
