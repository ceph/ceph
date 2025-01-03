import { Component, OnInit, ViewChild } from '@angular/core';
import { catchError, switchMap } from 'rxjs/operators';
import { BehaviorSubject, Observable, of } from 'rxjs';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { Permission } from '~/app/shared/models/permissions';

import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { UsersGroups } from '../smb.model';

@Component({
  selector: 'cd-smb-users-list',
  templateUrl: './smb-usersgroups-list.component.html',
  styleUrls: ['./smb-usersgroups-list.component.scss']
})
export class SmbUsersgroupsListComponent extends ListWithDetails implements OnInit {
  @ViewChild('table', { static: true })
  table: TableComponent;
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  usersGroups$: Observable<UsersGroups[]>;
  subject$ = new BehaviorSubject<UsersGroups[]>([]);

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private smbService: SmbService
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().smb;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`ID`,
        prop: 'users_groups_id',
        flexGrow: 2
      }
    ];

    this.usersGroups$ = this.subject$.pipe(
      switchMap(() =>
        this.smbService.listUsersGroups().pipe(
          catchError(() => {
            this.context.error();
            return of(null);
          })
        )
      )
    );
  }

  loadUsersGroups() {
    this.subject$.next([]);
  }
}
