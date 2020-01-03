import { Component } from '@angular/core';

import { UserPasswordFormComponent } from '../user-password-form/user-password-form.component';

@Component({
  selector: 'cd-user-password-login-form',
  templateUrl: './user-password-login-form.component.html',
  styleUrls: ['./user-password-login-form.component.scss']
})
export class UserPasswordLoginFormComponent extends UserPasswordFormComponent {}
