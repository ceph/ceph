import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { SettingsService } from '~/app/shared/api/settings.service';
import { UserService } from '~/app/shared/api/user.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { EmptyPipe } from '~/app/shared/pipes/empty.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { URLBuilderService } from '~/app/shared/services/url-builder.service';

const BASE_URL = 'user-management/users';

@Component({
  selector: 'cd-user-list',
  templateUrl: './user-list.component.html',
  styleUrls: ['./user-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class UserListComponent implements OnInit {
  @ViewChild('userRolesTpl', { static: true })
  userRolesTpl: TemplateRef<any>;
  @ViewChild('warningTpl', { static: true })
  warningTpl: TemplateRef<any>;
  @ViewChild('durationTpl', { static: true })
  durationTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[];
  users: Array<any>;
  expirationWarningAlert: number;
  expirationDangerAlert: number;
  selection = new CdTableSelection();
  icons = Icons;

  modalRef: NgbModalRef;

  constructor(
    private userService: UserService,
    private emptyPipe: EmptyPipe,
    private modalService: ModalCdsService,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private urlBuilder: URLBuilderService,
    private settingsService: SettingsService,
    public actionLabels: ActionLabelsI18n
  ) {
    this.permission = this.authStorageService.getPermissions().user;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      routerLink: () =>
        this.selection.first() && this.urlBuilder.getEdit(this.selection.first().username),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      click: () => this.deleteUserModal(),
      name: this.actionLabels.DELETE
    };
    this.tableActions = [addAction, editAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: $localize`Username`,
        prop: 'username',
        flexGrow: 1,
        cellTemplate: this.warningTpl
      },
      {
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 1,
        pipe: this.emptyPipe
      },
      {
        name: $localize`Email`,
        prop: 'email',
        flexGrow: 1,
        pipe: this.emptyPipe
      },
      {
        name: $localize`Roles`,
        prop: 'roles',
        flexGrow: 1,
        cellTemplate: this.userRolesTpl
      },
      {
        name: $localize`Enabled`,
        prop: 'enabled',
        flexGrow: 1,
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: $localize`Password expires`,
        prop: 'pwdExpirationDate',
        flexGrow: 1,
        cellTemplate: this.durationTpl
      }
    ];
    const settings: string[] = ['USER_PWD_EXPIRATION_WARNING_1', 'USER_PWD_EXPIRATION_WARNING_2'];
    this.settingsService.getValues(settings).subscribe((data) => {
      this.expirationWarningAlert = data['USER_PWD_EXPIRATION_WARNING_1'];
      this.expirationDangerAlert = data['USER_PWD_EXPIRATION_WARNING_2'];
    });
  }

  getUsers() {
    this.userService.list().subscribe((users: Array<any>) => {
      users.forEach((user) => {
        user['remainingTimeWithoutSeconds'] = 0;
        if (user['pwdExpirationDate'] && user['pwdExpirationDate'] > 0) {
          user['pwdExpirationDate'] = user['pwdExpirationDate'] * 1000;
          user['remainingTimeWithoutSeconds'] = this.getRemainingTimeWithoutSeconds(
            user.pwdExpirationDate
          );
          user['remainingDays'] = this.getRemainingDays(user.pwdExpirationDate);
        }
      });
      this.users = users;
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteUser(username: string) {
    this.userService.delete(username).subscribe(
      () => {
        this.getUsers();
        this.modalService.dismissAll();
        this.notificationService.show(
          NotificationType.success,
          $localize`Deleted user '${username}'`
        );
      },
      () => {
        this.modalRef.componentInstance.stopLoadingSpinner();
      }
    );
  }

  deleteUserModal() {
    const sessionUsername = this.authStorageService.getUsername();
    const username = this.selection.first().username;
    if (sessionUsername === username) {
      this.notificationService.show(
        NotificationType.error,
        $localize`Failed to delete user '${username}'`,
        $localize`You are currently logged in as '${username}'.`
      );
      return;
    }

    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
      impact: DeletionImpact.high,
      itemDescription: 'User',
      itemNames: [username],
      submitAction: () => this.deleteUser(username)
    });
  }

  getWarningIconClass(expirationDays: number): any {
    if (expirationDays === null || this.expirationWarningAlert > 10) {
      return '';
    }
    const remainingDays = this.getRemainingDays(expirationDays);
    if (remainingDays <= this.expirationDangerAlert) {
      return 'icon-danger-color';
    } else {
      return 'icon-warning-color';
    }
  }

  getWarningClass(expirationDays: number): any {
    if (expirationDays === null || this.expirationWarningAlert > 10) {
      return '';
    }
    const remainingDays = this.getRemainingDays(expirationDays);
    if (remainingDays <= this.expirationDangerAlert) {
      return 'border-danger';
    } else {
      return 'border-warning';
    }
  }

  getRemainingDays(time: number): number {
    if (time === undefined || time == null) {
      return undefined;
    }
    if (time < 0) {
      return 0;
    }
    const toDays = 1000 * 60 * 60 * 24;
    return Math.max(0, Math.floor(this.getRemainingTime(time) / toDays));
  }

  getRemainingTimeWithoutSeconds(time: number): number {
    const withSeconds = this.getRemainingTime(time);
    return Math.floor(withSeconds / (1000 * 60)) * 60 * 1000;
  }

  getRemainingTime(time: number): number {
    return time - Date.now();
  }
}
