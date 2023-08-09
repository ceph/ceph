import { Component, OnInit } from '@angular/core';
import { Permissions } from '~/app/shared/models/permissions';
import { Router } from '@angular/router';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

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

  constructor(
    private authStorageService: AuthStorageService,
    private cephfsService: CephfsService,
    private cdDatePipe: CdDatePipe,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private urlBuilder: URLBuilderService
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
        pipe: this.cdDatePipe,
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
      }
    ];
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
}
