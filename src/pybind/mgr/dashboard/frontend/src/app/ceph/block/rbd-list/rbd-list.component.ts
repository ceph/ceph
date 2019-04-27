import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

import { RbdService } from '../../../shared/api/rbd.service';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';
import { RbdParentModel } from '../rbd-form/rbd-parent.model';
import { RbdTrashMoveModalComponent } from '../rbd-trash-move-modal/rbd-trash-move-modal.component';
import { RbdModel } from './rbd-model';

const BASE_URL = 'block/rbd';

@Component({
  selector: 'cd-rbd-list',
  templateUrl: './rbd-list.component.html',
  styleUrls: ['./rbd-list.component.scss'],
  providers: [
    TaskListService,
    { provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }
  ]
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
  tableActions: CdTableAction[];
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
    private taskListService: TaskListService,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().rbdImage;
    const getImageUri = () =>
      this.selection.first() &&
      `${encodeURIComponent(this.selection.first().pool_name)}/${encodeURIComponent(
        this.selection.first().name
      )}`;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: 'fa-plus',
      routerLink: () => this.urlBuilder.getCreate(),
      canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection,
      name: this.actionLabels.CREATE
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      routerLink: () => this.urlBuilder.getEdit(getImageUri()),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-times',
      click: () => this.deleteRbdModal(),
      name: this.actionLabels.DELETE
    };
    const copyAction: CdTableAction = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: 'fa-copy',
      routerLink: () => `/block/rbd/copy/${getImageUri()}`,
      name: this.i18n('Copy')
    };
    const flattenAction: CdTableAction = {
      permission: 'update',
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting || !selection.first().parent,
      icon: 'fa-chain-broken',
      click: () => this.flattenRbdModal(),
      name: this.i18n('Flatten')
    };
    const moveAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-trash-o',
      click: () => this.trashRbdModal(),
      name: this.i18n('Move to Trash')
    };
    this.tableActions = [
      addAction,
      editAction,
      copyAction,
      flattenAction,
      deleteAction,
      moveAction
    ];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'name',
        flexGrow: 2,
        cellTransformation: CellTemplate.executing
      },
      {
        name: this.i18n('Pool'),
        prop: 'pool_name',
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
        name: this.i18n('Objects'),
        prop: 'num_objs',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessPipe
      },
      {
        name: this.i18n('Object size'),
        prop: 'obj_size',
        flexGrow: 1,
        cellClass: 'text-right',
        pipe: this.dimlessBinaryPipe
      },
      {
        name: this.i18n('Provisioned'),
        prop: 'disk_usage',
        cellClass: 'text-center',
        flexGrow: 1,
        pipe: this.dimlessBinaryPipe
      },
      {
        name: this.i18n('Total provisioned'),
        prop: 'total_disk_usage',
        cellClass: 'text-center',
        flexGrow: 1,
        pipe: this.dimlessBinaryPipe
      },
      {
        name: this.i18n('Parent'),
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
    let pool_name_k: string;
    let image_name_k: string;
    switch (task.name) {
      case 'rbd/copy':
        pool_name_k = 'dest_pool_name';
        image_name_k = 'dest_image_name';
        break;
      case 'rbd/clone':
        pool_name_k = 'child_pool_name';
        image_name_k = 'child_image_name';
        break;
      default:
        pool_name_k = 'pool_name';
        image_name_k = 'image_name';
        break;
    }
    return (
      entry.pool_name === task.metadata[pool_name_k] && entry.name === task.metadata[image_name_k]
    );
  }

  taskFilter(task) {
    return [
      'rbd/clone',
      'rbd/copy',
      'rbd/create',
      'rbd/delete',
      'rbd/edit',
      'rbd/flatten',
      'rbd/trash/move'
    ].includes(task.name);
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteRbdModal() {
    const poolName = this.selection.first().pool_name;
    const imageName = this.selection.first().name;

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'RBD',
        submitActionObservable: () =>
          this.taskWrapper.wrapTaskAroundCall({
            task: new FinishedTask('rbd/delete', {
              pool_name: poolName,
              image_name: imageName
            }),
            call: this.rbdService.delete(poolName, imageName)
          })
      }
    });
  }

  trashRbdModal() {
    const initialState = {
      metaType: 'RBD',
      poolName: this.selection.first().pool_name,
      imageName: this.selection.first().name
    };
    this.modalRef = this.modalService.show(RbdTrashMoveModalComponent, { initialState });
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
