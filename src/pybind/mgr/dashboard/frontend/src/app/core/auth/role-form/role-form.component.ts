import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin } from 'rxjs';

import { RoleService } from '../../../shared/api/role.service';
import { ScopeService } from '../../../shared/api/scope.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdForm } from '../../../shared/forms/cd-form';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { NotificationService } from '../../../shared/services/notification.service';
import { RoleFormMode } from './role-form-mode.enum';
import { RoleFormModel } from './role-form.model';

@Component({
  selector: 'cd-role-form',
  templateUrl: './role-form.component.html',
  styleUrls: ['./role-form.component.scss']
})
export class RoleFormComponent extends CdForm implements OnInit {
  @ViewChild('headerPermissionCheckboxTpl', { static: true })
  headerPermissionCheckboxTpl: TemplateRef<any>;
  @ViewChild('cellScopeCheckboxTpl', { static: true })
  cellScopeCheckboxTpl: TemplateRef<any>;
  @ViewChild('cellPermissionCheckboxTpl', { static: true })
  cellPermissionCheckboxTpl: TemplateRef<any>;

  modalRef: BsModalRef;

  roleForm: CdFormGroup;
  response: RoleFormModel;

  columns: CdTableColumn[];
  scopes: Array<string> = [];
  scopes_permissions: Array<any> = [];

  roleFormMode = RoleFormMode;
  mode: RoleFormMode;

  action: string;
  resource: string;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private roleService: RoleService,
    private scopeService: ScopeService,
    private notificationService: NotificationService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.resource = this.i18n('role');
    this.createForm();
    this.listenToChanges();
  }

  createForm() {
    this.roleForm = new CdFormGroup({
      name: new FormControl('', {
        validators: [Validators.required],
        asyncValidators: [CdValidators.unique(this.roleService.exists, this.roleService)]
      }),
      description: new FormControl(''),
      scopes_permissions: new FormControl({})
    });
  }

  ngOnInit() {
    this.columns = [
      {
        prop: 'scope',
        name: this.i18n('All'),
        flexGrow: 2,
        cellTemplate: this.cellScopeCheckboxTpl,
        headerTemplate: this.headerPermissionCheckboxTpl
      },
      {
        prop: 'read',
        name: this.i18n('Read'),
        flexGrow: 1,
        cellClass: 'text-center',
        cellTemplate: this.cellPermissionCheckboxTpl,
        headerTemplate: this.headerPermissionCheckboxTpl
      },
      {
        prop: 'create',
        name: this.i18n('Create'),
        flexGrow: 1,
        cellClass: 'text-center',
        cellTemplate: this.cellPermissionCheckboxTpl,
        headerTemplate: this.headerPermissionCheckboxTpl
      },
      {
        prop: 'update',
        name: this.i18n('Update'),
        flexGrow: 1,
        cellClass: 'text-center',
        cellTemplate: this.cellPermissionCheckboxTpl,
        headerTemplate: this.headerPermissionCheckboxTpl
      },
      {
        prop: 'delete',
        name: this.i18n('Delete'),
        flexGrow: 1,
        cellClass: 'text-center',
        cellTemplate: this.cellPermissionCheckboxTpl,
        headerTemplate: this.headerPermissionCheckboxTpl
      }
    ];
    if (this.router.url.startsWith('/user-management/roles/edit')) {
      this.mode = this.roleFormMode.editing;
      this.action = this.actionLabels.EDIT;
    } else {
      this.action = this.actionLabels.CREATE;
    }
    if (this.mode === this.roleFormMode.editing) {
      this.initEdit();
    } else {
      this.initCreate();
    }
  }

  initCreate() {
    // Load the scopes and initialize the default scopes/permissions data.
    this.scopeService.list().subscribe((scopes: Array<string>) => {
      this.scopes = scopes;
      this.roleForm.get('scopes_permissions').setValue({});

      this.loadingReady();
    });
  }

  initEdit() {
    // Disable the 'Name' input field.
    this.roleForm.get('name').disable();
    // Load the scopes and the role data.
    this.route.params.subscribe((params: { name: string }) => {
      const observables = [];
      observables.push(this.scopeService.list());
      observables.push(this.roleService.get(params.name));
      observableForkJoin(observables).subscribe((resp: any[]) => {
        this.scopes = resp[0];
        ['name', 'description', 'scopes_permissions'].forEach((key) =>
          this.roleForm.get(key).setValue(resp[1][key])
        );

        this.loadingReady();
      });
    });
  }

  listenToChanges() {
    // Create/Update the data which is used by the data table to display the
    // scopes/permissions every time the form field value has been changed.
    this.roleForm.get('scopes_permissions').valueChanges.subscribe((value) => {
      const scopes_permissions: any[] = [];
      _.each(this.scopes, (scope) => {
        // Set the defaults values.
        const scope_permission: any = { read: false, create: false, update: false, delete: false };
        scope_permission['scope'] = scope;
        // Apply settings from the given value if they exist.
        if (scope in value) {
          _.each(value[scope], (permission) => {
            scope_permission[permission] = true;
          });
        }
        scopes_permissions.push(scope_permission);
      });
      this.scopes_permissions = scopes_permissions;
    });
  }

  /**
   * Checks if the specified row checkbox needs to be rendered as checked.
   * @param {string} scope The scope to be checked, e.g. 'cephfs', 'grafana',
   *   'osd', 'pool' ...
   * @return Returns true if all permissions (read, create, update, delete)
   *   are checked for the specified scope, otherwise false.
   */
  isRowChecked(scope: string) {
    const scope_permission = _.find(this.scopes_permissions, (o) => {
      return o['scope'] === scope;
    });
    if (_.isUndefined(scope_permission)) {
      return false;
    }
    return (
      scope_permission['read'] &&
      scope_permission['create'] &&
      scope_permission['update'] &&
      scope_permission['delete']
    );
  }

  /**
   * Checks if the specified header checkbox needs to be rendered as checked.
   * @param {string} property The property/permission (read, create,
   *   update, delete) to be checked. If 'scope' is given, all permissions
   *   are checked.
   * @return Returns true if specified property/permission is selected
   *   for all scopes, otherwise false.
   */
  isHeaderChecked(property: string) {
    let permissions = [property];
    if ('scope' === property) {
      permissions = ['read', 'create', 'update', 'delete'];
    }
    return permissions.every((permission) => {
      return this.scopes_permissions.every((scope_permission) => {
        return scope_permission[permission];
      });
    });
  }

  onClickCellCheckbox(scope: string, property: string, event: any = null) {
    // Use a copy of the form field data to do not trigger the redrawing of the
    // data table with every change.
    const scopes_permissions = _.cloneDeep(this.roleForm.getValue('scopes_permissions'));
    let permissions = [property];
    if ('scope' === property) {
      permissions = ['read', 'create', 'update', 'delete'];
    }
    if (!(scope in scopes_permissions)) {
      scopes_permissions[scope] = [];
    }
    // Add or remove the given permission(s) depending on the click event or if no
    // click event is given then add/remove them if they are absent/exist.
    if (
      (event && event.target['checked']) ||
      !_.isEqual(permissions.sort(), _.intersection(scopes_permissions[scope], permissions).sort())
    ) {
      scopes_permissions[scope] = _.union(scopes_permissions[scope], permissions);
    } else {
      scopes_permissions[scope] = _.difference(scopes_permissions[scope], permissions);
      if (_.isEmpty(scopes_permissions[scope])) {
        _.unset(scopes_permissions, scope);
      }
    }
    this.roleForm.get('scopes_permissions').setValue(scopes_permissions);
  }

  onClickHeaderCheckbox(property: 'scope' | 'read' | 'create' | 'update' | 'delete', event: any) {
    // Use a copy of the form field data to do not trigger the redrawing of the
    // data table with every change.
    const scopes_permissions = _.cloneDeep(this.roleForm.getValue('scopes_permissions'));
    let permissions = [property];
    if ('scope' === property) {
      permissions = ['read', 'create', 'update', 'delete'];
    }
    _.each(permissions, (permission) => {
      _.each(this.scopes, (scope) => {
        if (event.target['checked']) {
          scopes_permissions[scope] = _.union(scopes_permissions[scope], [permission]);
        } else {
          scopes_permissions[scope] = _.difference(scopes_permissions[scope], [permission]);
          if (_.isEmpty(scopes_permissions[scope])) {
            _.unset(scopes_permissions, scope);
          }
        }
      });
    });
    this.roleForm.get('scopes_permissions').setValue(scopes_permissions);
  }

  getRequest(): RoleFormModel {
    const roleFormModel = new RoleFormModel();
    ['name', 'description', 'scopes_permissions'].forEach(
      (key) => (roleFormModel[key] = this.roleForm.get(key).value)
    );
    return roleFormModel;
  }

  createAction() {
    const roleFormModel = this.getRequest();
    this.roleService.create(roleFormModel).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n(`Created role '{{role_name}}'`, { role_name: roleFormModel.name })
        );
        this.router.navigate(['/user-management/roles']);
      },
      () => {
        this.roleForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  editAction() {
    const roleFormModel = this.getRequest();
    this.roleService.update(roleFormModel).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n(`Updated role '{{role_name}}'`, { role_name: roleFormModel.name })
        );
        this.router.navigate(['/user-management/roles']);
      },
      () => {
        this.roleForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  submit() {
    if (this.mode === this.roleFormMode.editing) {
      this.editAction();
    } else {
      this.createAction();
    }
  }
}
