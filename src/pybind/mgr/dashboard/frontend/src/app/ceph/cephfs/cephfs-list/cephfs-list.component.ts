import { Component, OnInit } from '@angular/core';
import { Permissions } from '~/app/shared/models/permissions';
import { Router } from '@angular/router';

import _ from 'lodash';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { CephfsMountDetailsComponent } from '../cephfs-mount-details/cephfs-mount-details.component';
import { map, switchMap } from 'rxjs/operators';
import { HealthService } from '~/app/shared/api/health.service';
import { CephfsAuthModalComponent } from '~/app/ceph/cephfs/cephfs-auth-modal/cephfs-auth-modal.component';

const BASE_URL = 'cephfs';

@Component({
  selector: 'cd-cephfs-list',
  templateUrl: './cephfs-list.component.html',
  styleUrls: ['./cephfs-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class CephfsListComponent extends ListWithDetails implements OnInit {
  columns: CdTableColumn[];
  filesystems: any = [];
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  permissions: Permissions;
  icons = Icons;
  monAllowPoolDelete = false;
  modalRef!: NgbModalRef;

  constructor(
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private urlBuilder: URLBuilderService,
    private configurationService: ConfigurationService,
    private modalService: ModalService,
    private taskWrapper: TaskWrapperService,
    public notificationService: NotificationService,
    private healthService: HealthService
  ) {
    super();
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'mdsmap.fs_name',
        flexGrow: 2
      },
      {
        name: $localize`Enabled`,
        prop: 'mdsmap.enabled',
        flexGrow: 2,
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: $localize`Created`,
        prop: 'mdsmap.created',
        flexGrow: 1,
        cellTransformation: CellTemplate.timeAgo
      }
    ];
    this.tableActions = [
      {
        name: this.actionLabels.CREATE,
        permission: 'create',
        icon: Icons.add,
        click: () => this.router.navigate([this.urlBuilder.getCreate()]),
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        name: this.actionLabels.EDIT,
        permission: 'update',
        icon: Icons.edit,
        click: () =>
          this.router.navigate([this.urlBuilder.getEdit(String(this.selection.first().id))])
      },
      {
        name: this.actionLabels.AUTHORIZE,
        permission: 'update',
        icon: Icons.edit,
        click: () => this.authorizeModal()
      },
      {
        name: this.actionLabels.ATTACH,
        permission: 'read',
        icon: Icons.bars,
        disable: () => !this.selection?.hasSelection,
        click: () => this.showAttachInfo()
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.removeVolumeModal(),
        name: this.actionLabels.REMOVE,
        disable: this.getDisableDesc.bind(this)
      }
    ];

    if (this.permissions.configOpt.read) {
      this.configurationService.get('mon_allow_pool_delete').subscribe((data: any) => {
        if (_.has(data, 'value')) {
          const monSection = _.find(data.value, (v) => {
            return v.section === 'mon';
          }) || { value: false };
          this.monAllowPoolDelete = monSection.value === 'true' ? true : false;
        }
      });
    }
  }

  loadFilesystems(context: CdTableFetchDataContext) {
    this.cephfsService.list().subscribe(
      (resp: any[]) => {
        this.filesystems = resp;
      },
      () => {
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  showAttachInfo() {
    const selectedFileSystem = this.selection?.selected?.[0];

    this.cephfsService
      .getFsRootDirectory(selectedFileSystem.id)
      .pipe(
        switchMap((fsData) =>
          this.healthService.getClusterFsid().pipe(map((data) => ({ clusterId: data, fs: fsData })))
        )
      )
      .subscribe({
        next: (val) => {
          this.modalRef = this.modalService.show(CephfsMountDetailsComponent, {
            onSubmit: () => this.modalRef.close(),
            mountData: {
              fsId: val.clusterId,
              fsName: selectedFileSystem?.mdsmap?.fs_name,
              rootPath: val.fs['path']
            }
          });
        }
      });
  }

  removeVolumeModal() {
    const volName = this.selection.first().mdsmap['fs_name'];
    this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'File System',
      itemNames: [volName],
      actionDescription: 'remove',
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/remove', { volumeName: volName }),
          call: this.cephfsService.remove(volName)
        })
    });
  }

  getDisableDesc(): boolean | string {
    if (this.selection?.hasSelection) {
      if (!this.monAllowPoolDelete) {
        return $localize`File System deletion is disabled by the mon_allow_pool_delete configuration setting.`;
      }

      return false;
    }

    return true;
  }

  authorizeModal() {
    const selectedFileSystem = this.selection?.selected?.[0];
    this.modalService.show(
      CephfsAuthModalComponent,
      {
        fsName: selectedFileSystem.mdsmap['fs_name'],
        id: selectedFileSystem.id
      },
      { size: 'lg' }
    );
  }
}
