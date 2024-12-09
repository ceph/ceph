import { Component, OnInit, ViewChild } from '@angular/core';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Accounts } from '../models/rgw-user-accounts';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';

@Component({
  selector: 'cd-rgw-user-accounts',
  templateUrl: './rgw-user-accounts.component.html',
  styleUrls: ['./rgw-user-accounts.component.scss']
})
export class RgwUserAccountsComponent implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  permission: Permission;
  tableActions: CdTableAction[] = [];
  columns: CdTableColumn[] = [];
  accounts: Accounts[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private rgwUserAccountsService: RgwUserAccountsService
  ) {}

  ngOnInit() {
    this.permission = this.authStorageService.getPermissions().rgw;
    this.columns = [
      {
        name: $localize`Account Id`,
        prop: 'id',
        flexGrow: 1
      },
      {
        name: $localize`Tenant`,
        prop: 'tenant',
        flexGrow: 1
      },
      {
        name: $localize`Full name`,
        prop: 'name',
        flexGrow: 1
      },
      {
        name: $localize`Email address`,
        prop: 'email',
        flexGrow: 1
      },
      {
        name: $localize`Max Users`,
        prop: 'max_users',
        flexGrow: 1
      },
      {
        name: $localize`Max Roles`,
        prop: 'max_roles',
        flexGrow: 1
      },
      {
        name: $localize`Max Groups`,
        prop: 'max_groups',
        flexGrow: 1
      },
      {
        name: $localize`Max. buckets`,
        prop: 'max_buckets',
        flexGrow: 1
      },
      {
        name: $localize`Max Access Keys`,
        prop: 'max_access_keys',
        flexGrow: 1
      }
    ];
  }

  getAccountsList(context: CdTableFetchDataContext) {
    this.rgwUserAccountsService.list(true).subscribe({
      next: (accounts) => {
        this.accounts = accounts;
      },
      error: () => {
        if (context) {
          context.error();
        }
      }
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
