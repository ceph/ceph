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
import { Observable, Subscriber, of } from 'rxjs';
import { map } from 'rxjs/operators';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwAccountRolePolicyFormComponent } from '../rgw-account-role-policy-form/rgw-account-role-policy-form.component';

import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DurationPipe } from '~/app/shared/pipes/duration.pipe';
import { RgwRole } from '../models/rgw-role';

@Component({
  selector: 'cd-rgw-account-roles-list',
  templateUrl: './rgw-account-roles-list.component.html',
  styleUrls: ['./rgw-account-roles-list.component.scss'],
  standalone: false
})
export class RgwAccountRolesListComponent implements OnInit, OnChanges {
  @Input()
  accountId: string;

  @Input()
  accountName: string;

  @ViewChild('table')
  table: TableComponent;

  columns: CdTableColumn[] = [];
  data$: Observable<RgwRole[]>;
  tableActions: CdTableAction[] = [];
  selection: CdTableSelection = new CdTableSelection();
  permission: Permission;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private rgwRoleService: RgwRoleService,
    private modalService: ModalCdsService,
    private authStorageService: AuthStorageService,
    private cdDatePipe: CdDatePipe,
    private durationPipe: DurationPipe,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().rgw;
  }

  ngOnInit(): void {
    this.loadRoles();
    this.columns = [
      {
        name: $localize`Name`,
        prop: 'RoleName',
        flexGrow: 2
      },
      {
        name: $localize`Policies`,
        prop: 'policies_count',
        flexGrow: 1
      },
      {
        name: $localize`Max session duration`,
        prop: 'MaxSessionDuration',
        flexGrow: 2,
        pipe: this.durationPipe
      },
      {
        name: $localize`Created`,
        prop: 'CreateDate',
        flexGrow: 2,
        pipe: this.cdDatePipe
      }
    ];

    this.tableActions = [
      {
        permission: 'create',
        icon: Icons.add,
        click: () => this.openRoleForm(false),
        name: this.actionLabels.CREATE,
        canBePrimary: (selection: CdTableSelection) => !selection.hasSelection
      },
      {
        permission: 'update',
        icon: Icons.edit,
        click: () => this.openRoleForm(true),
        name: $localize`Edit role`,
        disable: () => !this.selection.hasSelection
      },
      {
        permission: 'update',
        icon: Icons.add,
        click: () => this.openAttachPolicyModal(),
        name: $localize`Attach permission`,
        disable: () => !this.selection.hasSelection
      },
      {
        permission: 'delete',
        icon: Icons.destroy,
        click: () => this.deleteRole(),
        name: this.actionLabels.DELETE,
        disable: () => !this.selection.hasSelection
      }
    ];
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.accountId) {
      this.loadRoles();
    }
  }

  loadRoles(): void {
    if (!this.accountId) {
      this.data$ = of([]);
      return;
    }
    this.data$ = this.rgwRoleService.list(this.accountId).pipe(
      map((roles: RgwRole[]) => {
        return (roles || []).map((role) => {
          let count = (role as any).policies_count;
          if (count === undefined && (role as any).PermissionPolicies) {
            count = (role as any).PermissionPolicies.length;
          }
          return {
            ...role,
            policies_count: count ?? 0
          };
        });
      })
    );
  }

  expandedRow: RgwRole;

  updateSelection(selection: CdTableSelection): void {
    this.selection = selection;
  }

  setExpandedRow(event: any): void {
    this.expandedRow = event?.row || event;
  }

  openRoleForm(isEdit: boolean): void {
    const role = isEdit ? this.selection.first() : null;
    const modalRef = this.modalService.show(RgwAccountRoleFormComponent, {
      accountId: this.accountId,
      accountName: this.accountName,
      roleName: role ? role.RoleName : '',
      isEdit: isEdit,
      role: role
    });
    modalRef?.close?.subscribe(() => this.loadRoles());
  }

  openAttachPolicyModal(): void {
    const role = this.selection.first();
    if (!role) {
      return;
    }
    const modalRef = this.modalService.show(RgwAccountRolePolicyFormComponent, {
      accountId: this.accountId,
      roleName: role.RoleName
    });
    modalRef?.close?.subscribe(() => this.loadRoles());
  }

  deleteRole(): void {
    const roleName = this.selection.first().RoleName;
    this.modalService.show(DeleteConfirmationModalComponent, {
      itemDescription: $localize`Role`,
      itemNames: [roleName],
      submitActionObservable: () => {
        return new Observable((observer: Subscriber<any>) => {
          this.rgwRoleService.delete(roleName, this.accountId).subscribe({
            next: () => {
              this.notificationService.show(
                NotificationType.success,
                $localize`Role deleted successfully`
              );
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
