import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { catchError, switchMap } from 'rxjs/operators';
import { BehaviorSubject, Observable, of } from 'rxjs';

import _ from 'lodash';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { Permission } from '~/app/shared/models/permissions';

import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { SMBUsersGroups } from '../smb.model';

@Component({
  selector: 'cd-smb-users-list',
  templateUrl: './smb-usersgroups-list.component.html',
  styleUrls: ['./smb-usersgroups-list.component.scss']
})
export class SmbUsersgroupsListComponent extends ListWithDetails implements OnInit {
  @ViewChild('groupsNamesTpl', { static: true })
  groupsNamesTpl: TemplateRef<any>;
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  usersGroups$: Observable<SMBUsersGroups[]>;
  subject$ = new BehaviorSubject<SMBUsersGroups[]>([]);

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
      },
      {
        name: $localize`Number of users`,
        prop: 'values.users.length',
        flexGrow: 2
      },
      {
        name: $localize`Groups`,
        prop: 'values.groups',
        cellTemplate: this.groupsNamesTpl,
        flexGrow: 2
      },
      {
        name: $localize`Linked to`,
        prop: 'values.linked_to_cluster',
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
