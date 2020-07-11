import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { RbdService } from '../../../shared/api/rbd.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { FinishedTask } from '../../../shared/models/finished-task';
import { ImageSpec } from '../../../shared/models/image-spec';
import { Permission } from '../../../shared/models/permissions';
import { Task } from '../../../shared/models/task';
import { DimlessBinaryPipe } from '../../../shared/pipes/dimless-binary.pipe';
import { DimlessPipe } from '../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';
import { RbdParentModel } from '../rbd-form/rbd-parent.model';
import { RbdTrashMoveModalComponent } from '../rbd-trash-move-modal/rbd-trash-move-modal.component';
import { RBDImageFormat, RbdModel } from './rbd-model';

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
export class RbdListComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  @ViewChild('usageTpl')
  usageTpl: TemplateRef<any>;
  @ViewChild('parentTpl', { static: true })
  parentTpl: TemplateRef<any>;
  @ViewChild('nameTpl')
  nameTpl: TemplateRef<any>;
  @ViewChild('flattenTpl', { static: true })
  flattenTpl: TemplateRef<any>;
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  images: any;
  columns: CdTableColumn[];
  retries: number;
  viewCacheStatusList: any[];
  selection = new CdTableSelection();

  modalRef: NgbModalRef;

  builders = {
    'rbd/create': (metadata: object) =>
      this.createRbdFromTask(metadata['pool_name'], metadata['namespace'], metadata['image_name']),
    'rbd/delete': (metadata: object) => this.createRbdFromTaskImageSpec(metadata['image_spec']),
    'rbd/clone': (metadata: object) =>
      this.createRbdFromTask(
        metadata['child_pool_name'],
        metadata['child_namespace'],
        metadata['child_image_name']
      ),
    'rbd/copy': (metadata: object) =>
      this.createRbdFromTask(
        metadata['dest_pool_name'],
        metadata['dest_namespace'],
        metadata['dest_image_name']
      )
  };

  private createRbdFromTaskImageSpec(imageSpecStr: string): RbdModel {
    const imageSpec = ImageSpec.fromString(imageSpecStr);
    return this.createRbdFromTask(imageSpec.poolName, imageSpec.namespace, imageSpec.imageName);
  }

  private createRbdFromTask(pool: string, namespace: string, name: string): RbdModel {
    const model = new RbdModel();
    model.id = '-1';
    model.unique_id = '-1';
    model.name = name;
    model.namespace = namespace;
    model.pool_name = pool;
    model.image_format = RBDImageFormat.V2;
    return model;
  }

  constructor(
    private authStorageService: AuthStorageService,
    private rbdService: RbdService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private dimlessPipe: DimlessPipe,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    private taskListService: TaskListService,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().rbdImage;
    const getImageUri = () =>
      this.selection.first() &&
      new ImageSpec(
        this.selection.first().pool_name,
        this.selection.first().namespace,
        this.selection.first().name
      ).toStringEncoded();
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection,
      name: this.actionLabels.CREATE
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () => this.urlBuilder.getEdit(getImageUri()),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteRbdModal(),
      name: this.actionLabels.DELETE,
      disable: (selection: CdTableSelection) =>
        !this.selection.first() ||
        !this.selection.hasSingleSelection ||
        this.hasClonedSnapshots(selection.first()),
      disableDesc: () => this.getDeleteDisableDesc()
    };
    const copyAction: CdTableAction = {
      permission: 'create',
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      icon: Icons.copy,
      routerLink: () => `/block/rbd/copy/${getImageUri()}`,
      name: this.actionLabels.COPY
    };
    const flattenAction: CdTableAction = {
      permission: 'update',
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting || !selection.first().parent,
      icon: Icons.flatten,
      click: () => this.flattenRbdModal(),
      name: this.actionLabels.FLATTEN
    };
    const moveAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.trash,
      click: () => this.trashRbdModal(),
      name: this.actionLabels.TRASH,
      disable: (selection: CdTableSelection) =>
        !selection.first() ||
        !selection.hasSingleSelection ||
        selection.first().image_format === RBDImageFormat.V1
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
        name: this.i18n('Namespace'),
        prop: 'namespace',
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

    const itemFilter = (entry: Record<string, any>, task: Task) => {
      let taskImageSpec: string;
      switch (task.name) {
        case 'rbd/copy':
          taskImageSpec = new ImageSpec(
            task.metadata['dest_pool_name'],
            task.metadata['dest_namespace'],
            task.metadata['dest_image_name']
          ).toString();
          break;
        case 'rbd/clone':
          taskImageSpec = new ImageSpec(
            task.metadata['child_pool_name'],
            task.metadata['child_namespace'],
            task.metadata['child_image_name']
          ).toString();
          break;
        case 'rbd/create':
          taskImageSpec = new ImageSpec(
            task.metadata['pool_name'],
            task.metadata['namespace'],
            task.metadata['image_name']
          ).toString();
          break;
        default:
          taskImageSpec = task.metadata['image_spec'];
          break;
      }
      return (
        taskImageSpec === new ImageSpec(entry.pool_name, entry.namespace, entry.name).toString()
      );
    };

    const taskFilter = (task: Task) => {
      return [
        'rbd/clone',
        'rbd/copy',
        'rbd/create',
        'rbd/delete',
        'rbd/edit',
        'rbd/flatten',
        'rbd/trash/move'
      ].includes(task.name);
    };

    this.taskListService.init(
      () => this.rbdService.list(),
      (resp) => this.prepareResponse(resp),
      (images) => (this.images = images),
      () => this.onFetchError(),
      taskFilter,
      itemFilter,
      this.builders
    );
  }

  onFetchError() {
    this.table.reset(); // Disable loading indicator.
    this.viewCacheStatusList = [{ status: ViewCacheStatus.ValueException }];
  }

  prepareResponse(resp: any[]): any[] {
    let images: any[] = [];
    const viewCacheStatusMap = {};
    resp.forEach((pool) => {
      if (_.isUndefined(viewCacheStatusMap[pool.status])) {
        viewCacheStatusMap[pool.status] = [];
      }
      viewCacheStatusMap[pool.status].push(pool.pool_name);
      images = images.concat(pool.value);
    });
    const viewCacheStatusList: any[] = [];
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

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteRbdModal() {
    const poolName = this.selection.first().pool_name;
    const namespace = this.selection.first().namespace;
    const imageName = this.selection.first().name;
    const imageSpec = new ImageSpec(poolName, namespace, imageName);

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'RBD',
      itemNames: [imageSpec],
      bodyTemplate: this.deleteTpl,
      bodyContext: {
        hasSnapshots: this.hasSnapshots(),
        snapshots: this.listProtectedSnapshots()
      },
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('rbd/delete', {
            image_spec: imageSpec.toString()
          }),
          call: this.rbdService.delete(imageSpec)
        })
    });
  }

  trashRbdModal() {
    const initialState = {
      poolName: this.selection.first().pool_name,
      namespace: this.selection.first().namespace,
      imageName: this.selection.first().name,
      hasSnapshots: this.hasSnapshots()
    };
    this.modalRef = this.modalService.show(RbdTrashMoveModalComponent, initialState);
  }

  flattenRbd(imageSpec: ImageSpec) {
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('rbd/flatten', {
          image_spec: imageSpec.toString()
        }),
        call: this.rbdService.flatten(imageSpec)
      })
      .subscribe({
        complete: () => {
          this.modalRef.close();
        }
      });
  }

  flattenRbdModal() {
    const poolName = this.selection.first().pool_name;
    const namespace = this.selection.first().namespace;
    const imageName = this.selection.first().name;
    const parent: RbdParentModel = this.selection.first().parent;
    const parentImageSpec = new ImageSpec(
      parent.pool_name,
      parent.pool_namespace,
      parent.image_name
    );
    const childImageSpec = new ImageSpec(poolName, namespace, imageName);

    const initialState = {
      titleText: 'RBD flatten',
      buttonText: 'Flatten',
      bodyTpl: this.flattenTpl,
      bodyData: {
        parent: `${parentImageSpec}@${parent.snap_name}`,
        child: childImageSpec.toString()
      },
      onSubmit: () => {
        this.flattenRbd(childImageSpec);
      }
    };

    this.modalRef = this.modalService.show(ConfirmationModalComponent, initialState);
  }

  hasSnapshots() {
    const snapshots = this.selection.first()['snapshots'] || [];
    return snapshots.length > 0;
  }

  hasClonedSnapshots(image: object) {
    const snapshots = image['snapshots'] || [];
    return snapshots.some((snap: object) => snap['children'] && snap['children'].length > 0);
  }

  listProtectedSnapshots() {
    const first = this.selection.first();
    const snapshots = first['snapshots'];
    return snapshots.reduce((accumulator: string[], snap: object) => {
      if (snap['is_protected']) {
        accumulator.push(snap['name']);
      }
      return accumulator;
    }, []);
  }

  getDeleteDisableDesc(): string {
    const first = this.selection.first();
    if (first && this.hasClonedSnapshots(first)) {
      return this.i18n(
        'This RBD has cloned snapshots. Please delete related RBDs before deleting this RBD.'
      );
    }

    return '';
  }
}
