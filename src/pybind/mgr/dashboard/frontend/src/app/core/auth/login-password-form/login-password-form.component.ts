import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { AuthService } from '~/app/shared/api/auth.service';
import { UserService } from '~/app/shared/api/user.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PasswordPolicyService } from '~/app/shared/services/password-policy.service';
import { UserPasswordFormComponent } from '../user-password-form/user-password-form.component';

@Component({
  selector: 'cd-login-password-form',
  templateUrl: './login-password-form.component.html',
  styleUrls: ['./login-password-form.component.scss']
})
export class LoginPasswordFormComponent extends UserPasswordFormComponent {
  constructor(
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    public userService: UserService,
    public authStorageService: AuthStorageService,
    public formBuilder: CdFormBuilder,
    public router: Router,
    public passwordPolicyService: PasswordPolicyService,
    public authService: AuthService
  ) {
    super(
      actionLabels,
      notificationService,
      userService,
      authStorageService,
      formBuilder,
      router,
      passwordPolicyService
    );
  }

  onPasswordChange() {
    // Logout here because changing the password will change the
    // session token which will finally lead to a 401 when calling
    // the REST API the next time. The API HTTP interceptor will
    // then also redirect to the login page immediately.
    this.authService.logout();
  }

  onCancel() {
    this.authService.logout();
  }
}
