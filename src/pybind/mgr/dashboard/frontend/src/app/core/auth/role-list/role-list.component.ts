import { Component, OnInit } from '@angular/core';

import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin } from 'rxjs';

import { RoleService } from '~/app/shared/api/role.service';
import { ScopeService } from '~/app/shared/api/scope.service';
import { ListWithDetails } from '~/app/shared/classes/list-with-details.class';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
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
    private modalService: ModalCdsService,
    private notificationService: NotificationService,
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
        name: $localize`Name`,
        prop: 'name',
        flexGrow: 3
      },
      {
        name: $localize`Description`,
        prop: 'description',
        flexGrow: 5,
        pipe: this.emptyPipe
      },
      {
        name: $localize`System Role`,
        prop: 'system',
        cellClass: 'text-left',
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
        this.modalService.dismissAll();
        this.notificationService.show(NotificationType.success, $localize`Deleted role '${role}'`);
      },
      () => {
        this.modalRef.componentInstance.stopLoadingSpinner();
      }
    );
  }

  deleteRoleModal() {
    const name = this.selection.first().name;
    this.modalRef = this.modalService.show(DeleteConfirmationModalComponent, {
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
          label: $localize`New name`,
          required: true
        }
      ],
      titleText: $localize`Clone Role`,
      submitButtonText: $localize`Clone Role`,
      onSubmit: (values: object) => {
        this.roleService.clone(name, values['newName']).subscribe(() => {
          this.getRoles();
          this.notificationService.show(
            NotificationType.success,
            $localize`Cloned role '${values['newName']}' from '${name}'`
          );
        });
      }
    });
  }
}
