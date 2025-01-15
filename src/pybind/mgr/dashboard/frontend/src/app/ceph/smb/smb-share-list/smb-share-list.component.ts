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
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-smb-share-list',
  templateUrl: './smb-share-list.component.html',
  styleUrls: ['./smb-share-list.component.scss']
})
export class SmbShareListComponent implements OnInit {
  @Input()
  clusterId: string;
  @ViewChild('table', { static: true })
  table: TableComponent;
  columns: CdTableColumn[];
  permission: Permission;
  selection: CdTableSelection;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  smbShares$: Observable<SMBShare[]>;
  subject$ = new BehaviorSubject<SMBShare[]>([]);
  modalRef: NgbModalRef;

  constructor(
    private authStorageService: AuthStorageService,
    private taskWrapper: TaskWrapperService,
    private smbService: SmbService,
    public actionLabels: ActionLabelsI18n,
    private modalService: ModalCdsService
  ) {
    this.permission = this.authStorageService.getPermissions().smb;
  }

  ngOnInit() {
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
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteShareModal(),
        name: this.actionLabels.DELETE
      }
    ];

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

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteShareModal() {
    const cluster_id = this.selection.first().cluster_id;
    const share_id = this.selection.first().share_id;
    const name = this.selection.first().name;

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: $localize`SMB Share`,
      itemNames: [`${share_id}(${name}) from cluster: ${cluster_id}`],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('cephfs/smb/share/delete', {
            cluster_id: cluster_id,
            share_id: share_id
          }),
          call: this.smbService.deleteShare(cluster_id, share_id)
        })
    });
  }
}
