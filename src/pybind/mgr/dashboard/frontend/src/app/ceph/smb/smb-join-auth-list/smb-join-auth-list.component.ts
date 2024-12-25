import { Component, OnInit } from '@angular/core';
import { Observable, BehaviorSubject, of } from 'rxjs';
import { switchMap, catchError } from 'rxjs/operators';
import { SmbService } from '~/app/shared/api/smb.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SMBJoinAuth } from '../smb.model';

@Component({
  selector: 'cd-smb-join-auth-list',
  templateUrl: './smb-join-auth-list.component.html',
  styleUrls: ['./smb-join-auth-list.component.scss']
})
export class SmbJoinAuthListComponent implements OnInit {
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  joinAuth$: Observable<SMBJoinAuth[]>;
  subject$ = new BehaviorSubject<SMBJoinAuth[]>([]);

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private smbService: SmbService
  ) {
    this.permission = this.authStorageService.getPermissions().smb;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`ID`,
        prop: 'auth_id',
        flexGrow: 2
      },
      {
        name: $localize`Username`,
        prop: 'auth.username',
        flexGrow: 2
      },
      {
        name: $localize`Linked to Cluster`,
        prop: 'linked_to_cluster',
        flexGrow: 2
      }
    ];

    this.joinAuth$ = this.subject$.pipe(
      switchMap(() =>
        this.smbService.listJoinAuths().pipe(
          catchError(() => {
            this.context.error();
            return of(null);
          })
        )
      )
    );
  }

  loadJoinAuth() {
    this.subject$.next([]);
  }
}
