import { Component, OnInit } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { forkJoin } from 'rxjs';

import { RoleService } from '../../../shared/api/role.service';
import { ScopeService } from '../../../shared/api/scope.service';
import { ListWithDetails } from '../../../shared/classes/list-with-details.class';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '../../../shared/components/form-modal/form-modal.component';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { EmptyPipe } from '../../../shared/pipes/empty.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ModalService } from '../../../shared/services/modal.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { URLBuilderService } from '../../../shared/services/url-builder.service';

const BASE_URL = 'user-management/roles';

@Component({
  selector: 'cd-role-list',
  templateUrl: './role-list.component.html',
  styleUrls: ['./role-list.component.scss'],
  providers: [{ provide: URLBuilderService, useValue: new URLBuilderService(BASE_URL) }]
})
export class RoleListComponent extends ListWithDetails implements OnInit {
  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[];
  roles: Array<any>;
  scopes: Array<string>;
  selection = new CdTableSelection();

  modalRef: NgbModalRef;

  constructor(
    private roleService: RoleService,
    private scopeService: ScopeService,
    private emptyPipe: EmptyPipe,
    private authStorageService: AuthStorageService,
    private modalService: ModalService,
    private notificationService: NotificationService,
    private i18n: I18n,
    private urlBuilder: URLBuilderService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().user;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: Icons.add,
      routerLink: () => this.urlBuilder.getCreate(),
      name: this.actionLabels.CREATE
    };
    const cloneAction: CdTableAction = {
      permission: 'create',
      icon: Icons.clone,
      name: this.actionLabels.CLONE,
      disable: () => !this.selection.hasSingleSelection,
      click: () => this.cloneRole()
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: Icons.edit,
      disable: () => !this.selection.hasSingleSelection || this.selection.first().system,
      routerLink: () =>
        this.selection.first() && this.urlBuilder.getEdit(this.selection.first().name),
      name: this.actionLabels.EDIT
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: Icons.destroy,
      disable: () => !this.selection.hasSingleSelection || this.selection.first().system,
      click: () => this.deleteRoleModal(),
      name: this.actionLabels.DELETE
    };
    this.tableActions = [addAction, cloneAction, editAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'name',
        flexGrow: 3
      },
      {
        name: this.i18n('Description'),
        prop: 'description',
        flexGrow: 5,
        pipe: this.emptyPipe
      },
      {
        name: this.i18n('System Role'),
        prop: 'system',
        cellClass: 'text-center',
        flexGrow: 1,
        cellTransformation: CellTemplate.checkIcon
      }
    ];
  }

  getRoles() {
    forkJoin([this.roleService.list(), this.scopeService.list()]).subscribe(
      (data: [Array<any>, Array<string>]) => {
        this.roles = data[0];
        this.scopes = data[1];
      }
    );
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }

  deleteRole(role: string) {
    this.roleService.delete(role).subscribe(
      () => {
        this.getRoles();
        this.modalRef.close();
        this.notificationService.show(
          NotificationType.success,
          this.i18n(`Deleted role '{{role_name}}'`, { role_name: role })
        );
      },
      () => {
        this.modalRef.componentInstance.stopLoadingSpinner();
      }
    );
  }

  deleteRoleModal() {
    const name = this.selection.first().name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      itemDescription: 'Role',
      itemNames: [name],
      submitAction: () => this.deleteRole(name)
    });
  }

  cloneRole() {
    const name = this.selection.first().name;
    this.modalRef = this.modalService.show(FormModalComponent, {
      fields: [
        {
          type: 'text',
          name: 'newName',
          value: `${name}_clone`,
          label: this.i18n('New name'),
          required: true
        }
      ],
      titleText: this.i18n('Clone Role'),
      submitButtonText: this.i18n('Clone Role'),
      onSubmit: (values: object) => {
        this.roleService.clone(name, values['newName']).subscribe(() => {
          this.getRoles();
          this.notificationService.show(
            NotificationType.success,
            this.i18n(`Cloned role '{{dst_name}}' from '{{src_name}}'`, {
              src_name: name,
              dst_name: values['newName']
            })
          );
        });
      }
    });
  }
}
