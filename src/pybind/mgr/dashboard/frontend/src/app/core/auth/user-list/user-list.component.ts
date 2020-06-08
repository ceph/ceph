import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';

import { UserService } from '../../../shared/api/user.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { CdDatePipe } from '../../../shared/pipes/cd-date.pipe';
import { EmptyPipe } from '../../../shared/pipes/empty.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';

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
  @ViewChild('userEnabledTpl', { static: true })
  userEnabledTpl: TemplateRef<any>;

  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[];
  users: Array<any>;
  selection = new CdTableSelection();

  modalRef: BsModalRef;

  constructor(
    private userService: UserService,
    private emptyPipe: EmptyPipe,
    private modalService: BsModalService,
    private notificationService: NotificationService,
    private authStorageService: AuthStorageService,
    private i18n: I18n,
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
        name: this.i18n('Username'),
        prop: 'username',
        flexGrow: 1
      },
      {
        name: this.i18n('Name'),
        prop: 'name',
        flexGrow: 1,
        pipe: this.emptyPipe
      },
      {
        name: this.i18n('Email'),
        prop: 'email',
        flexGrow: 1,
        pipe: this.emptyPipe
      },
      {
        name: this.i18n('Roles'),
        prop: 'roles',
        flexGrow: 1,
        cellTemplate: this.userRolesTpl
      },
      {
        name: this.i18n('Enabled'),
        prop: 'enabled',
        flexGrow: 1,
        cellTransformation: CellTemplate.checkIcon
      },
      {
        name: this.i18n('Password expiration date'),
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
        this.modalRef.hide();
        this.notificationService.show(
          NotificationType.success,
          this.i18n(`Deleted user '{{username}}'`, { username: username })
        );
      },
      () => {
        this.modalRef.content.stopLoadingSpinner();
      }
    );
  }

  deleteUserModal() {
    const sessionUsername = this.authStorageService.getUsername();
    const username = this.selection.first().username;
    if (sessionUsername === username) {
      this.notificationService.show(
        NotificationType.error,
        this.i18n(`Failed to delete user '{{username}}'`, { username: username }),
        this.i18n(`You are currently logged in as '{{username}}'.`, { username: username })
      );
      return;
    }
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'User',
        itemNames: [username],
        submitAction: () => this.deleteUser(username)
      }
    });
  }
}
