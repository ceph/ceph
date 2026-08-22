import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Permissions } from '~/app/shared/models/permissions';
import { Router } from '@angular/router';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CephfsActionService } from '~/app/shared/services/cephfs-action.service';

const BASE_URL = 'cephfs/fs';

@Component({
  selector: 'cd-cephfs-list',
  templateUrl: './cephfs-list.component.html',
  styleUrls: ['./cephfs-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }],
  standalone: false
})
export class CephfsListComponent implements OnInit {
  @ViewChild('deleteTpl', { static: true })
  deleteTpl: TemplateRef<any>;

  columns: CdTableColumn[];
  filesystems: any = [];
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  permissions: Permissions;
  icons = Icons;
  monAllowPoolDelete = false;
  readonly basePath = '/cephfs/fs';

  constructor(
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private urlBuilder: URLBuilderService,
    public notificationService: NotificationService,
    private cephfsActionService: CephfsActionService
  ) {
    this.permissions = this.authStorageService.getPermissions();
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'mdsmap.fs_name',
        flexGrow: 2,
        cellTransformation: CellTemplate.routerLink
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
        click: () => this.cephfsActionService.authorize(this.selection?.selected?.[0])
      },
      {
        name: this.actionLabels.ATTACH,
        permission: 'read',
        icon: Icons.bars,
        disable: () => !this.selection?.hasSelection,
        click: () => this.cephfsActionService.showAttachInfo(this.selection?.selected?.[0])
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () =>
          this.cephfsActionService.removeVolume(
            this.selection.first().mdsmap['fs_name'],
            this.deleteTpl
          ),
        name: this.actionLabels.REMOVE,
        disable: this.getDisableDesc.bind(this)
      }
    ];

    if (this.permissions.configOpt.read) {
      this.cephfsActionService.getMonAllowPoolDelete().subscribe((monAllowPoolDelete: boolean) => {
        this.monAllowPoolDelete = monAllowPoolDelete;
      });
    }
  }

  loadFilesystems(context: CdTableFetchDataContext) {
    this.cephfsService.list().subscribe(
      (resp: any[]) => {
        this.filesystems = (resp || []).map((filesystem: any) => ({
          ...filesystem,
          cdLink: `${this.basePath}/${filesystem.id}/overview`
        }));
      },
      () => {
        context.error();
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  getDisableDesc(): boolean | string {
    return this.cephfsActionService.getDeleteDisableDesc(
      this.selection?.hasSelection,
      this.monAllowPoolDelete
    );
  }
}
