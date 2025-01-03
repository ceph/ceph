import { Component, OnInit, ViewChild } from '@angular/core';
import { Observable, BehaviorSubject, of } from 'rxjs';
import { switchMap, catchError } from 'rxjs/operators';
import { SmbService } from '~/app/shared/api/smb.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { JoinAuth } from '../smb.model';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';

@Component({
  selector: 'cd-smb-join-auth-list',
  templateUrl: './smb-join-auth-list.component.html',
  styleUrls: ['./smb-join-auth-list.component.scss']
})
export class SmbJoinAuthListComponent extends ListWithDetails implements OnInit {
  @ViewChild('table', { static: true })
  table: TableComponent;
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  joinAuth$: Observable<JoinAuth[]>;
  subject$ = new BehaviorSubject<JoinAuth[]>([]);

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
        prop: 'auth_id',
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
