import { Component, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from '@angular/core';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { RgwAccountRoleFormComponent } from '../rgw-account-role-form/rgw-account-role-form.component';
import { Observable, Subscriber } from 'rxjs';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DurationPipe } from '~/app/shared/pipes/duration.pipe';

@Component({
  selector: 'cd-rgw-account-roles-list',
  templateUrl: './rgw-account-roles-list.component.html',
  styleUrls: ['./rgw-account-roles-list.component.scss'],
  standalone: false
})
export class RgwAccountRolesListComponent implements OnInit, OnChanges {
  @Input()
  accountId: string;

  @ViewChild('table')
  table: TableComponent;

  columns: CdTableColumn[] = [];
  data: any[] = [];
  tableActions: CdTableAction[] = [];
  selection: CdTableSelection = new CdTableSelection();
  permission: Permission;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private rgwRoleService: RgwRoleService,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private cdDatePipe: CdDatePipe,
    private durationPipe: DurationPipe
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit(): void {
    this.loadRoles();
    this.columns = [
      {
        name: $localize`Role Name`,
        prop: 'RoleName',
        flexGrow: 2
      },
      {
        name: $localize`Path`,
        prop: 'Path',
        flexGrow: 2
      },
      {
        name: $localize`Arn`,
        prop: 'Arn',
        flexGrow: 3
      },
      {
        name: $localize`Create Date`,
        prop: 'CreateDate',
        flexGrow: 2,
        pipe: this.cdDatePipe
      },
      {
        name: $localize`Max Session Duration`,
        prop: 'MaxSessionDuration',
        flexGrow: 2,
        pipe: this.durationPipe
      }
    ];

    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      click: () => this.openRoleForm(false),
      name: this.actionLabels.CREATE,
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
    };

    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      click: () => this.openRoleForm(true),
      name: this.actionLabels.EDIT
    };

    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteRole(),
      name: this.actionLabels.DELETE,
      disable: () => !this.selection.hasSelection
    };

    this.tableActions = [addAction, editAction, deleteAction];
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.accountId) {
      this.loadRoles();
    }
  }

  loadRoles(): void {
    if (!this.accountId) {
      this.data = [];
      return;
    }
    this.rgwRoleService.list(this.accountId).subscribe((roles: any[]) => {
      this.data = roles;
    });
  }

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  openRoleForm(isEdit: boolean): void {
    const role = isEdit ? this.selection.first() : null;
    const modalRef = this.modalService.show(RgwAccountRoleFormComponent, {
      accountId: this.accountId,
      roleName: role ? role.RoleName : '',
      isEdit: isEdit
    });
    modalRef?.close?.subscribe(() => this.loadRoles());
  }

  deleteRole(): void {
    const roleName = this.selection.first().RoleName;
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`RGW Role`,
      itemNames: [roleName],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwRoleService.delete(roleName, this.accountId).subscribe({
            next: () => {
              observer.next();
              observer.complete();
              this.loadRoles();
            },
            error: (err) => {
              observer.error(err);
            }
          });
        });
      }
    });
  }
}
