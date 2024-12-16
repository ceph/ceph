import { Component, Input, OnInit, ViewChild } from '@angular/core';
import { Observable, BehaviorSubject, of } from 'rxjs';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { Permission } from '~/app/shared/models/permissions';
import { SMBShare } from '../smb.model';
import { switchMap, catchError } from 'rxjs/operators';
import { SmbService } from '~/app/shared/api/smb.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-smb-share-list',
  templateUrl: './smb-share-list.component.html'
})
export class SmbShareListComponent implements OnInit {
  @Input()
  clusterId: string;
  @ViewChild('table', { static: true })
  table: TableComponent;
  columns: CdTableColumn[];
  permission: Permission;
  selection = new CdTableSelection();
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  smbShares$: Observable<SMBShare[]>;
  subject$ = new BehaviorSubject<SMBShare[]>([]);

  constructor(
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private smbService: SmbService,
    private urlBuilder: URLBuilderService
  ) {
    this.permission = this.authStorageService.getPermissions().smb;
  }

  ngOnInit() {
    console.log(this.urlBuilder.getCreate());
    this.columns = [
      {
        name: $localize`ID`,
        prop: 'share_id',
        flexGrow: 2
      },
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 2
      },
      {
        name: $localize`File System`,
        prop: 'cephfs.volume',
        flexGrow: 2
      },
      {
        name: $localize`Path`,
        prop: 'cephfs.path',
        flexGrow: 2
      },
      {
        name: $localize`Subvolumegroup`,
        prop: 'cephfs.subvolumegroup',
        flexGrow: 2
      },
      {
        name: $localize`Subvolume`,
        prop: 'cephfs.subvolume',
        flexGrow: 2
      },
      {
        name: $localize`Provider`,
        prop: 'cephfs.provider',
        flexGrow: 2
      }
    ];
    this.tableActions = [
      {
        name: `${this.actionLabels.CREATE} Share`,
        permission: 'create',
        icon: Icons.add,
        routerLink: () =>[ '/cephfs/smb/share/create',this.clusterId],
        
        canBePrimary: (selection: CdTableSelection) => !selection.hasSingleSelection
      }
    ];
console.log(this.clusterId, "cluster");
    this.smbShares$ = this.subject$.pipe(
      switchMap(() =>
        this.smbService.listShares(this.clusterId).pipe(
          catchError(() => {
            this.context.error();
            return of(null);
          })
        )
      )
    );
  }

  loadSMBShares() {
    this.subject$.next([]);
  }
}
