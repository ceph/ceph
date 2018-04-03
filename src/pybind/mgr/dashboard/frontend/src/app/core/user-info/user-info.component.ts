import { Location } from '@angular/common';
import { Component, OnInit } from '@angular/core';
import {
  FormControl,
  FormGroup,
  Validators
} from '@angular/forms';
import { Router } from '@angular/router';

import { AuthService } from '../../shared/api/auth.service';
import { UserService } from '../../shared/api/user.service';
import { NotificationType } from '../../shared/enum/notification-type.enum';
import { User } from '../../shared/models/user';
import { AuthStorageService } from '../../shared/services/auth-storage.service';
import { NotificationService } from '../../shared/services/notification.service';
import { CdValidators } from '../../shared/validators/cd-validators';

@Component({
  selector: 'cd-user-info',
  templateUrl: './user-info.component.html',
  styleUrls: ['./user-info.component.scss']
})
export class UserInfoComponent implements OnInit {
  userForm: FormGroup;
  username: string;

  constructor(
    private location: Location,
    private authStorageService: AuthStorageService,
    private userService: UserService,
    private notificationService: NotificationService,
    private authService: AuthService,
    private router: Router
  ) {
    this.userForm = new FormGroup(
      {
        username: new FormControl('', {
          validators: [Validators.required]
        }),
        password: new FormControl(),
        confirmPassword: new FormControl()
      },
      {
        validators: [CdValidators.matchPassword]
      }
    );
  }

  ngOnInit() {
    this.username = this.authStorageService.get();
    this.userService.get(this.username).subscribe((res: User) => {
      this.userForm.patchValue(res);
    });
  }

  submit() {
    const user = new User();
    user.setValues(this.userForm.value);

    this.userService.set(user).subscribe(
      resp => {
        this.notificationService.show(
          NotificationType.info,
          `Your authentication will be revoked in 5 seconds. \
          Please login again with your new username/password.`
        );
        this.notificationService.show(
          NotificationType.success,
          `Your user has been updated successfully`
        );

        setTimeout(() => {
          this.authService.logout().then(() => {
            this.router.navigate(['/login']);
          });
        }, 5000);
      },
      null,
      () => {
        this.userForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

  cancel() {
    this.location.back(); // <-- go back to previous location on cancel
  }
}
