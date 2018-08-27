import { Component, EventEmitter, Output } from '@angular/core';
import { AbstractControl, AsyncValidatorFn, ValidationErrors, Validators } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';
import { Observable, of as observableOf, timer as observableTimer } from 'rxjs';
import { map, switchMapTo, take } from 'rxjs/operators';

import { UserService } from '../../../shared/api/user.service';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';

@Component({
  selector: 'cd-user-clone-modal',
  templateUrl: './user-clone-modal.component.html',
  styleUrls: ['./user-clone-modal.component.scss']
})
export class UserCloneModalComponent {
  /**
   * The event that is triggered when the 'Clone' button as been pressed.
   */
  @Output()
  submitAction = new EventEmitter();

  formGroup: CdFormGroup;

  constructor(
    private formBuilder: CdFormBuilder,
    public bsModalRef: BsModalRef,
    private userService: UserService
  ) {
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({
      username: [null],
      new_username: [null, [Validators.required], [this.usernameValidator()]]
    });
  }

  /**
   * Set the values displayed in the dialog.
   */
  setValues(username: string = null, new_username: string = null) {
    this.formGroup.setValue({
      username: username,
      new_username: new_username
    });
  }

  onSubmit() {
    const new_username: string = this.formGroup.value.new_username;
    this.submitAction.emit(new_username);
    this.bsModalRef.hide();
  }

  onCancel() {
    this.bsModalRef.hide();
  }

  /**
   * Validate the username.
   * @param {number|Date} dueTime The delay time to wait before the
   *   API call is executed. This is useful to prevent API calls on
   *   every keystroke. Defaults to 500.
   */
  usernameValidator(dueTime = 500): AsyncValidatorFn {
    const userService = this.userService;
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      // Exit immediately if user has not interacted with the control yet
      // or the control value is empty.
      if (control.pristine || control.value === '') {
        return observableOf(null);
      }
      // Forgot previous requests if a new one arrives within the specified
      // delay time.
      return observableTimer(dueTime).pipe(
        switchMapTo(userService.exists(control.value)),
        map((resp: boolean) => {
          if (!resp) {
            return null;
          } else {
            return { usernameExists: true };
          }
        }),
        take(1)
      );
    };
  }
}
