import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { BsModalRef } from 'ngx-bootstrap';

import { RoleService } from '../../../shared/api/role.service';
import { ScopeService } from '../../../shared/api/scope.service';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { NotificationService } from '../../../shared/services/notification.service';
import { RoleFormMode } from './role-form-mode.enum';
import { RoleFormModel } from './role-form.model';

@Component({
  selector: 'cd-role-form',
  templateUrl: './role-form.component.html',
  styleUrls: ['./role-form.component.scss']
})
export class RoleFormComponent implements OnInit {
  modalRef: BsModalRef;

  roleForm: CdFormGroup;
  response: RoleFormModel;
  scopes: Array<string>;

  roleFormMode = RoleFormMode;
  mode: RoleFormMode;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private roleService: RoleService,
    private scopeService: ScopeService,
    private notificationService: NotificationService
  ) {
    this.createForm();
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
    if (this.router.url.startsWith('/user-management/roles/edit')) {
      this.mode = this.roleFormMode.editing;
    }
    this.scopeService.list().subscribe((scopes: Array<string>) => {
      this.scopes = scopes;
    });
    if (this.mode === this.roleFormMode.editing) {
      this.initEdit();
    }
  }

  initEdit() {
    this.disableForEdit();
    this.route.params.subscribe((params: { name: string }) => {
      const name = params.name;
      this.roleService.get(name).subscribe((roleFormModel: RoleFormModel) => {
        this.setResponse(roleFormModel);
      });
    });
  }

  disableForEdit() {
    this.roleForm.get('name').disable();
  }

  setResponse(response: RoleFormModel) {
    ['name', 'description', 'scopes_permissions'].forEach((key) =>
      this.roleForm.get(key).setValue(response[key])
    );
  }

  hadlePermissionClick(scope: string, permission: string) {
    const permissions = this.roleForm.getValue('scopes_permissions');
    if (!permissions[scope]) {
      permissions[scope] = [];
    }
    const index = permissions[scope].indexOf(permission);
    if (index === -1) {
      permissions[scope].push(permission);
    } else {
      permissions[scope].splice(index, 1);
    }
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
          `Created role '${roleFormModel.name}'`
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
          `Updated role '${roleFormModel.name}'`
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
