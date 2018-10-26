import { Component, OnInit } from '@angular/core';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin } from 'rxjs';

import { RoleService } from '../../../shared/api/role.service';
import { ScopeService } from '../../../shared/api/scope.service';
import { CriticalConfirmationModalComponent } from '../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { EmptyPipe } from '../../../shared/empty.pipe';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdTableAction } from '../../../shared/models/cd-table-action';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { Permission } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-role-list',
  templateUrl: './role-list.component.html',
  styleUrls: ['./role-list.component.scss']
})
export class RoleListComponent implements OnInit {
  permission: Permission;
  tableActions: CdTableAction[];
  columns: CdTableColumn[];
  roles: Array<any>;
  scopes: Array<string>;
  selection = new CdTableSelection();

  modalRef: BsModalRef;

  constructor(
    private roleService: RoleService,
    private scopeService: ScopeService,
    private emptyPipe: EmptyPipe,
    private authStorageService: AuthStorageService,
    private modalService: BsModalService,
    private notificationService: NotificationService
  ) {
    this.permission = this.authStorageService.getPermissions().user;
    const addAction: CdTableAction = {
      permission: 'create',
      icon: 'fa-plus',
      routerLink: () => '/user-management/roles/add',
      name: 'Add'
    };
    const editAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      disable: () => !this.selection.hasSingleSelection || this.selection.first().system,
      routerLink: () =>
        this.selection.first() && `/user-management/roles/edit/${this.selection.first().name}`,
      name: 'Edit'
    };
    const deleteAction: CdTableAction = {
      permission: 'delete',
      icon: 'fa-times',
      disable: () => !this.selection.hasSingleSelection || this.selection.first().system,
      click: () => this.deleteRoleModal(),
      name: 'Delete'
    };
    this.tableActions = [addAction, editAction, deleteAction];
  }

  ngOnInit() {
    this.columns = [
      {
        name: 'Name',
        prop: 'name',
        flexGrow: 3
      },
      {
        name: 'Description',
        prop: 'description',
        flexGrow: 5,
        pipe: this.emptyPipe
      },
      {
        name: 'System Role',
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
        this.modalRef.hide();
        this.notificationService.show(NotificationType.success, `Deleted role '${role}'`);
      },
      () => {
        this.modalRef.content.stopLoadingSpinner();
      }
    );
  }

  deleteRoleModal() {
    const name = this.selection.first().name;
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        itemDescription: 'Role',
        submitAction: () => this.deleteRole(name)
      }
    });
  }
}
