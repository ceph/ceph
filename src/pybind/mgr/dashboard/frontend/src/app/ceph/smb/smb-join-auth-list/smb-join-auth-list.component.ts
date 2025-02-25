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
import { Router } from '@angular/router';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

export const JOINAUTH_URL = '/cephfs/smb/ad';

@Component({
  selector: 'cd-smb-join-auth-list',
  templateUrl: './smb-join-auth-list.component.html',
  styleUrls: ['./smb-join-auth-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(JOINAUTH_URL) }]
})
export class SmbJoinAuthListComponent implements OnInit {
  columns: CdTableColumn[];
  permission: Permission;
  tableActions: CdTableAction[];
  context: CdTableFetchDataContext;

  joinAuth$: Observable<SMBJoinAuth[]>;
  subject$ = new BehaviorSubject<SMBJoinAuth[]>([]);
  selection: CdTableSelection = new CdTableSelection();

  constructor(
    private router: Router,
    private urlBuilder: URLBuilderService,
    private authStorageService: AuthStorageService,
    public actionLabels: ActionLabelsI18n,
    private smbService: SmbService,
    private modalService: ModalCdsService,
    private taskWrapper: TaskWrapperService
  ) {
    this.permission = this.authStorageService.getPermissions().smb;
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'auth_id',
        flexGrow: 2
      },
      {
        name: $localize`Username`,
        prop: 'auth.username',
        flexGrow: 2
      },
      {
        name: $localize`Linked to cluster`,
        prop: 'linked_to_cluster',
        flexGrow: 2
      }
    ];

    this.tableActions = [
      {
        name: `${this.actionLabels.CREATE} AD`,
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
          this.router.navigate([this.urlBuilder.getEdit(String(this.selection.first().auth_id))])
      },
      {
        name: this.actionLabels.DELETE,
        permission: 'update',
        icon: Icons.destroy,
        click: () => this.openDeleteModal()
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

  openDeleteModal() {
    const authId = this.selection.first().auth_id;

    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Active directory access resource`,
      itemNames: [authId],
      submitActionObservable: () =>
        this.taskWrapper.wrapTaskAroundCall({
          task: new FinishedTask('smb/ad/remove', {
            authId: authId
          }),
          call: this.smbService.deleteJoinAuth(authId)
        })
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}
