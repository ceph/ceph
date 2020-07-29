import { Component } from '@angular/core';
import { Validators } from '@angular/forms';
import { Router } from '@angular/router';

import * as _ from 'lodash';

import { UserService } from '../../../shared/api/user.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { PasswordPolicyService } from '../../../shared/services/password-policy.service';

@Component({
  selector: 'cd-user-password-form',
  templateUrl: './user-password-form.component.html',
  styleUrls: ['./user-password-form.component.scss']
})
export class UserPasswordFormComponent {
  userForm: CdFormGroup;
  action: string;
  resource: string;
  passwordPolicyHelpText = '';
  passwordStrengthLevelClass: string;
  passwordValuation: string;
  icons = Icons;

  constructor(
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    public userService: UserService,
    public authStorageService: AuthStorageService,
    public formBuilder: CdFormBuilder,
    public router: Router,
    public passwordPolicyService: PasswordPolicyService
  ) {
    this.action = this.actionLabels.CHANGE;
    this.resource = $localize`password`;
    this.createForm();
  }

  createForm() {
    this.passwordPolicyService.getHelpText().subscribe((helpText: string) => {
      this.passwordPolicyHelpText = helpText;
    });
    this.userForm = this.formBuilder.group(
      {
        oldpassword: [
          null,
          [
            Validators.required,
            CdValidators.custom('notmatch', () => {
              return (
                this.userForm &&
                this.userForm.getValue('newpassword') === this.userForm.getValue('oldpassword')
              );
            })
          ]
        ],
        newpassword: [
          null,
          [
            Validators.required,
            CdValidators.custom('notmatch', () => {
              return (
                this.userForm &&
                this.userForm.getValue('oldpassword') === this.userForm.getValue('newpassword')
              );
            })
          ],
          [
            CdValidators.passwordPolicy(
              this.userService,
              () => this.authStorageService.getUsername(),
              (_valid: boolean, credits: number, valuation: string) => {
                this.passwordStrengthLevelClass = this.passwordPolicyService.mapCreditsToCssClass(
                  credits
                );
                this.passwordValuation = _.defaultTo(valuation, '');
              }
            )
          ]
        ],
        confirmnewpassword: [null, [Validators.required]]
      },
      {
        validators: [CdValidators.match('newpassword', 'confirmnewpassword')]
      }
    );
  }

  onSubmit() {
    if (this.userForm.pristine) {
      return;
    }
    const username = this.authStorageService.getUsername();
    const oldPassword = this.userForm.getValue('oldpassword');
    const newPassword = this.userForm.getValue('newpassword');
    this.userService.changePassword(username, oldPassword, newPassword).subscribe(
      () => this.onPasswordChange(),
      () => {
        this.userForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  /**
   * The function that is called after the password has been changed.
   * Override this in derived classes to change the behaviour.
   */
  onPasswordChange() {
    this.notificationService.show(NotificationType.success, $localize`Updated user password"`);
    this.router.navigate(['/login']);
  }
}
