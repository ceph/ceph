import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin } from 'rxjs';

import { AuthService } from '../../../shared/api/auth.service';
import { RoleService } from '../../../shared/api/role.service';
import { SettingsService } from '../../../shared/api/settings.service';
import { UserService } from '../../../shared/api/user.service';
import { ConfirmationModalComponent } from '../../../shared/components/confirmation-modal/confirmation-modal.component';
import { SelectMessages } from '../../../shared/components/select/select-messages.model';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdForm } from '../../../shared/forms/cd-form';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { CdPwdExpirationSettings } from '../../../shared/models/cd-pwd-expiration-settings';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { PasswordPolicyService } from '../../../shared/services/password-policy.service';
import { UserFormMode } from './user-form-mode.enum';
import { UserFormRoleModel } from './user-form-role.model';
import { UserFormModel } from './user-form.model';

@Component({
  selector: 'cd-user-form',
  templateUrl: './user-form.component.html',
  styleUrls: ['./user-form.component.scss']
})
export class UserFormComponent extends CdForm implements OnInit {
  @ViewChild('removeSelfUserReadUpdatePermissionTpl', { static: true })
  removeSelfUserReadUpdatePermissionTpl: TemplateRef<any>;

  modalRef: BsModalRef;

  userForm: CdFormGroup;
  response: UserFormModel;

  userFormMode = UserFormMode;
  mode: UserFormMode;
  allRoles: Array<UserFormRoleModel>;
  messages = new SelectMessages({ empty: this.i18n('There are no roles.') }, this.i18n);
  action: string;
  resource: string;
  passwordPolicyHelpText = '';
  passwordStrengthLevelClass: string;
  passwordValuation: string;
  icons = Icons;
  minDate: Date;
  bsConfig = {
    dateInputFormat: 'YYYY-MM-DD',
    containerClass: 'theme-default'
  };
  pwdExpirationSettings: CdPwdExpirationSettings;

  constructor(
    private authService: AuthService,
    private authStorageService: AuthStorageService,
    private route: ActivatedRoute,
    private router: Router,
    private modalService: BsModalService,
    private roleService: RoleService,
    private userService: UserService,
    private notificationService: NotificationService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n,
    private passwordPolicyService: PasswordPolicyService,
    private formBuilder: CdFormBuilder,
    private settingsService: SettingsService
  ) {
    super();
    this.resource = this.i18n('user');
    this.createForm();
    this.messages = new SelectMessages({ empty: this.i18n('There are no roles.') }, this.i18n);
  }

  createForm() {
    this.passwordPolicyService.getHelpText().subscribe((helpText: string) => {
      this.passwordPolicyHelpText = helpText;
    });
    this.userForm = this.formBuilder.group(
      {
        username: [
          '',
          [Validators.required],
          [CdValidators.unique(this.userService.validateUserName, this.userService)]
        ],
        name: [''],
        password: [
          '',
          [],
          [
            CdValidators.passwordPolicy(
              this.userService,
              () => this.userForm.getValue('username'),
              (_valid: boolean, credits: number, valuation: string) => {
                this.passwordStrengthLevelClass = this.passwordPolicyService.mapCreditsToCssClass(
                  credits
                );
                this.passwordValuation = _.defaultTo(valuation, '');
              }
            )
          ]
        ],
        confirmpassword: [''],
        pwdExpirationDate: [''],
        email: ['', [CdValidators.email]],
        roles: [[]],
        enabled: [true, [Validators.required]],
        pwdUpdateRequired: [true]
      },
      {
        validators: [CdValidators.match('password', 'confirmpassword')]
      }
    );
  }

  ngOnInit() {
    if (this.router.url.startsWith('/user-management/users/edit')) {
      this.mode = this.userFormMode.editing;
      this.action = this.actionLabels.EDIT;
    } else {
      this.action = this.actionLabels.CREATE;
    }
    this.minDate = new Date();

    const observables = [this.roleService.list(), this.settingsService.getStandardSettings()];
    observableForkJoin(observables).subscribe(
      (result: [UserFormRoleModel[], CdPwdExpirationSettings]) => {
        this.allRoles = _.map(result[0], (role) => {
          role.enabled = true;
          return role;
        });
        this.pwdExpirationSettings = new CdPwdExpirationSettings(result[1]);

        if (this.mode === this.userFormMode.editing) {
          this.initEdit();
        } else {
          if (this.pwdExpirationSettings.pwdExpirationSpan > 0) {
            const pwdExpirationDateField = this.userForm.get('pwdExpirationDate');
            const expirationDate = new Date();
            expirationDate.setDate(
              this.minDate.getDate() + this.pwdExpirationSettings.pwdExpirationSpan
            );
            pwdExpirationDateField.setValue(expirationDate);
            pwdExpirationDateField.setValidators([Validators.required]);
          }

          this.loadingReady();
        }
      }
    );
  }

  initEdit() {
    this.disableForEdit();
    this.route.params.subscribe((params: { username: string }) => {
      const username = params.username;
      this.userService.get(username).subscribe((userFormModel: UserFormModel) => {
        this.response = _.cloneDeep(userFormModel);
        this.setResponse(userFormModel);

        this.loadingReady();
      });
    });
  }

  disableForEdit() {
    this.userForm.get('username').disable();
  }

  setResponse(response: UserFormModel) {
    ['username', 'name', 'email', 'roles', 'enabled', 'pwdUpdateRequired'].forEach((key) =>
      this.userForm.get(key).setValue(response[key])
    );
    const expirationDate = response['pwdExpirationDate'];
    if (expirationDate) {
      this.userForm.get('pwdExpirationDate').setValue(new Date(expirationDate * 1000));
    }
  }

  getRequest(): UserFormModel {
    const userFormModel = new UserFormModel();
    ['username', 'password', 'name', 'email', 'roles', 'enabled', 'pwdUpdateRequired'].forEach(
      (key) => (userFormModel[key] = this.userForm.get(key).value)
    );
    const expirationDate = this.userForm.get('pwdExpirationDate').value;
    if (expirationDate) {
      if (
        this.mode !== this.userFormMode.editing ||
        this.response.pwdExpirationDate !== Number(expirationDate) / 1000
      ) {
        expirationDate.setHours(23, 59, 59);
      }
      userFormModel['pwdExpirationDate'] = Number(expirationDate) / 1000;
    }
    return userFormModel;
  }

  createAction() {
    const userFormModel = this.getRequest();
    this.userService.create(userFormModel).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n('Created user "{{username}}"', { username: userFormModel.username })
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
        titleText: this.i18n('Update user'),
        buttonText: this.i18n('Continue'),
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

  public isCurrentUser(): boolean {
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
              this.i18n('You were automatically logged out because your roles have been changed.')
            );
          });
        } else {
          this.notificationService.show(
            NotificationType.success,
            this.i18n('Updated user "{{username}}"', { username: userFormModel.username })
          );
          this.router.navigate(['/user-management/users']);
        }
      },
      () => {
        this.userForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  clearExpirationDate() {
    this.userForm.get('pwdExpirationDate').setValue(undefined);
  }

  submit() {
    if (this.mode === this.userFormMode.editing) {
      this.editAction();
    } else {
      this.createAction();
    }
  }
}
