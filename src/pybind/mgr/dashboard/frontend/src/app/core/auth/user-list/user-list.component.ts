import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { UserService } from '~/app/shared/api/user.service';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { EmptyPipe } from '~/app/shared/pipes/empty.pipe';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
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

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[];
  users: Array<any>;
  selection = new CdTableSelection();

  modalRef: NgbModalRef;

  constructor(
    private userService: UserService,
    private emptyPipe: EmptyPipe,
    private modalService: ModalService,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private urlBuilder: URLBuilderService,
    private cdDatePipe: CdDatePipe,
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
        flexGrow: 1
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
        name: $localize`Password expiration date`,
        prop: 'pwdExpirationDate',
        flexGrow: 1,
        pipe: this.cdDatePipe
      }
    ];
  }

  getUsers() {
    this.userService.list().subscribe((users: Array<any>) => {
      users.forEach((user) => {
        if (user['pwdExpirationDate'] && user['pwdExpirationDate'] > 0) {
          user['pwdExpirationDate'] = user['pwdExpirationDate'] * 1000;
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
        this.modalRef.close();
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

    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'User',
      itemNames: [username],
      submitAction: () => this.deleteUser(username)
    });
  }
}
