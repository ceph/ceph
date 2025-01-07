import { Component, OnInit, ViewChild } from '@angular/core';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Account } from '../models/rgw-user-accounts';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { Router } from '@angular/router';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';

const BASE_URL = 'rgw/accounts';

@Component({
  selector: 'cd-rgw-user-accounts',
  templateUrl: './rgw-user-accounts.component.html',
  styleUrls: ['./rgw-user-accounts.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RgwUserAccountsComponent extends ListWithDetails implements OnInit {
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;
  permission: Permission;
  tableActions: CdTableAction[] = [];
  columns: CdTableColumn[] = [];
  accounts: Account[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private router: Router,
    private rgwUserAccountsService: RgwUserAccountsService
  ) {
    super();
  }

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
    const getEditURL = () => {
      if (this.selection.first().groupName && this.selection.first().id) {
        return `${URLVerbs.EDIT}/${this.selection.first().id}
        }`;
      }
      return `${URLVerbs.EDIT}/${this.selection.first().id}`;
    };
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      click: () => this.router.navigate([`${BASE_URL}/${URLVerbs.CREATE}`]),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      click: () => this.router.navigate([`${BASE_URL}/${getEditURL()}`]),
      name: this.actionLabels.EDIT
    };
    this.tableActions = [addAction, editAction];
  }

  getAccountsList(context?: CdTableFetchDataContext) {
    this.rgwUserAccountsService.list(true).subscribe({
      next: (accounts: Account[]) => {
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
