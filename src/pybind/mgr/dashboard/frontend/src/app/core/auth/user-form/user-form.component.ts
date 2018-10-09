import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { AuthService } from '../../../shared/api/auth.service';
import { RoleService } from '../../../shared/api/role.service';
import { UserService } from '../../../shared/api/user.service';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { SelectBadgesMessages } from '../../../shared/components/select-badges/select-badges-messages.model';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { UserFormMode } from './user-form-mode.enum';
import { UserFormRoleModel } from './user-form-role.model';
import { UserFormModel } from './user-form.model';

@Component({
  selector: 'cd-user-form',
  templateUrl: './user-form.component.html',
  styleUrls: ['./user-form.component.scss']
})
export class UserFormComponent implements OnInit {
  @ViewChild('removeSelfUserReadUpdatePermissionTpl')
  removeSelfUserReadUpdatePermissionTpl: TemplateRef<any>;

  modalRef: BsModalRef;

  userForm: CdFormGroup;
  response: UserFormModel;

  userFormMode = UserFormMode;
  mode: UserFormMode;
  allRoles: Array<UserFormRoleModel>;
  messages = new SelectBadgesMessages({ empty: 'There are no roles.' });

  constructor(
    private authService: AuthService,
    private authStorageService: AuthStorageService,
    private route: ActivatedRoute,
    private router: Router,
    private modalService: BsModalService,
    private roleService: RoleService,
    private userService: UserService,
    private notificationService: NotificationService
  ) {
    this.createForm();
  }

  createForm() {
    this.userForm = new CdFormGroup(
      {
        username: new FormControl('', {
          validators: [Validators.required]
        }),
        name: new FormControl(''),
        password: new FormControl('', {
          validators: []
        }),
        confirmpassword: new FormControl('', {
          updateOn: 'blur',
          validators: []
        }),
        email: new FormControl('', {
          validators: [Validators.email]
        }),
        roles: new FormControl([])
      },
      {
        validators: [CdValidators.match('password', 'confirmpassword')]
      }
    );
  }

  ngOnInit() {
    if (this.router.url.startsWith('/user-management/users/edit')) {
      this.mode = this.userFormMode.editing;
    }
    this.roleService.list().subscribe((roles: Array<UserFormRoleModel>) => {
      this.allRoles = roles;
    });
    if (this.mode === this.userFormMode.editing) {
      this.initEdit();
    }
  }

  initEdit() {
    this.disableForEdit();
    this.route.params.subscribe((params: { username: string }) => {
      const username = params.username;
      this.userService.get(username).subscribe((userFormModel: UserFormModel) => {
        this.response = _.cloneDeep(userFormModel);
        this.setResponse(userFormModel);
      });
    });
  }

  disableForEdit() {
    this.userForm.get('username').disable();
  }

  setResponse(response: UserFormModel) {
    ['username', 'name', 'email', 'roles'].forEach((key) =>
      this.userForm.get(key).setValue(response[key])
    );
  }

  getRequest(): UserFormModel {
    const userFormModel = new UserFormModel();
    ['username', 'password', 'name', 'email', 'roles'].forEach(
      (key) => (userFormModel[key] = this.userForm.get(key).value)
    );
    return userFormModel;
  }

  createAction() {
    const userFormModel = this.getRequest();
    this.userService.create(userFormModel).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          `Created user "${userFormModel.username}"`
        );
        this.router.navigate(['/user-management/users']);
      },
      () => {
        this.userForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  editAction() {
    if (this.isUserRemovingNeededRolePermissions()) {
      const initialState = {
        titleText: 'Update user',
        buttonText: 'Continue',
        bodyTpl: this.removeSelfUserReadUpdatePermissionTpl,
        onSubmit: () => {
          this.modalRef.hide();
          this.doEditAction();
        },
        onCancel: () => {
          this.userForm.setErrors({ cdSubmitButton: true });
          this.userForm.get('roles').reset(this.userForm.get('roles').value);
        }
      };
      this.modalRef = this.modalService.show(ConfirmationModalComponent, { initialState });
    } else {
      this.doEditAction();
    }
  }

  private isCurrentUser(): boolean {
    return this.authStorageService.getUsername() === this.userForm.getValue('username');
  }

  private isUserChangingRoles(): boolean {
    const isCurrentUser = this.isCurrentUser();
    return (
      isCurrentUser &&
      this.response &&
      !_.isEqual(this.response.roles, this.userForm.getValue('roles'))
    );
  }

  private isUserRemovingNeededRolePermissions(): boolean {
    const isCurrentUser = this.isCurrentUser();
    return isCurrentUser && !this.hasUserReadUpdatePermissions(this.userForm.getValue('roles'));
  }

  private hasUserReadUpdatePermissions(roles: Array<string> = []) {
    for (const role of this.allRoles) {
      if (roles.indexOf(role.name) !== -1 && role.scopes_permissions['user']) {
        const userPermissions = role.scopes_permissions['user'];
        return ['read', 'update'].every((permission) => {
          return userPermissions.indexOf(permission) !== -1;
        });
      }
    }
    return false;
  }

  private doEditAction() {
    const userFormModel = this.getRequest();
    this.userService.update(userFormModel).subscribe(
      () => {
        if (this.isUserChangingRoles()) {
          this.authService.logout(() => {
            this.notificationService.show(
              NotificationType.info,
              'You were automatically logged out because your roles have been changed.'
            );
            this.router.navigate(['/login']);
          });
        } else {
          this.notificationService.show(
            NotificationType.success,
            `Updated user "${userFormModel.username}"`
          );
          this.router.navigate(['/user-management/users']);
        }
      },
      () => {
        this.userForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  submit() {
    if (this.mode === this.userFormMode.editing) {
      this.editAction();
    } else {
      this.createAction();
    }
  }
}
