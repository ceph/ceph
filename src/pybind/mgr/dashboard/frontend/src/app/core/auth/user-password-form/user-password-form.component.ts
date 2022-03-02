import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import _ from 'lodash';

import { UserService } from '~/app/shared/api/user.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PasswordPolicyService } from '~/app/shared/services/password-policy.service';

@Component({
  selector: 'cd-user-password-form',
  templateUrl: './user-password-form.component.html',
  styleUrls: ['./user-password-form.component.scss']
})
export class UserPasswordFormComponent implements OnInit {
  userForm: CdFormGroup;
  action: string;
  resource: string;
  passwordPolicyHelpText = '';
  passwordStrengthLevelClass: string;
  passwordValuation: string;
  icons = Icons;
  errorObject: Object;
  passwordForm: any;

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
  }

  public formConfig = {
    options: {
      hooks: {
        beforeSubmit: (submission: any, callback: any) => {
          if(submission.data.oldPass === submission.data.newPass) {
          setTimeout(() => {
            callback(
              {
                message: "The old and new passwords must be different.",
                component: null
              },
              null
            );
          }, 1000);
         }
         if(submission.data.newPass !== submission.data.confirmPass) {
          setTimeout(() => {
            callback(
              {
                message: "Password confirmation doesn't match the new password.",
                component: null
              },
              null
            );
          }, 1000);
         }
         else {
         this.userService.validatePassword(submission.data.newPass, this.authStorageService.getUsername(), submission.data.oldPass).subscribe((resp:object) => {
           this.errorObject = resp;
           if(!this.errorObject['valid']) {
            setTimeout(() => {
             callback(
               {
                 message: this.errorObject['valuation'],
                 component: null
               },
               null
             );
           }, 1000);
         }
         else {
          this.onSubmit(submission);
         }
         });
        }
        }
      }
    },
  };

  ngOnInit() {
    this.passwordForm = {
      title: "Change Password Form",
      components: [
        {
          "type": "password",
          "inputType": "text",
          "label": "Old password",
          "key": "oldPass",
          "placeholder": "Enter old password",
          "validate": {
              "required": true,
          }
      },
        {
          "type": "password",
          "inputType": "text",
          "label": "New password",
          "key": "newPass",
          "placeholder": "Enter new password",
          "validate": {
              "required": true,
          }
      },
      {
        "type": "password",
        "inputType": "text",
        "label": "Confirm password",
        "key": "confirmPass",
        "placeholder": "Confirm password",
        "validate": {
            "required": true,
        }
      },
      {
          "label": "Change Password",
          "key": "submit",
          "action": "submit",
          "disableOnInvalid": true,
          "theme": "primary",
          "type": "button"
      }
    ]
    };
  }

  onSubmit(submission: any) {
    const username = this.authStorageService.getUsername();
    this.userService.changePassword(username, submission.data.oldPass, submission.data.newPass).subscribe(
      () => this.onPasswordChange(),
      () => {
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
