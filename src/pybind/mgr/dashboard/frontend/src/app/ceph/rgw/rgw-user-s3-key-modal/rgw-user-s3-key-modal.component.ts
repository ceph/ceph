import { Component, EventEmitter, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';

import { CdValidators } from '../../../shared/validators/cd-validators';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';

@Component({
  selector: 'cd-rgw-user-s3-key-modal',
  templateUrl: './rgw-user-s3-key-modal.component.html',
  styleUrls: ['./rgw-user-s3-key-modal.component.scss']
})
export class RgwUserS3KeyModalComponent {

  /**
   * The event that is triggered when the 'Add' button as been pressed.
   */
  @Output() submitAction = new EventEmitter();

  formGroup: FormGroup;
  viewing = true;
  userCandidates: string[] = [];

  constructor(private formBuilder: FormBuilder,
              public bsModalRef: BsModalRef) {
    this.createForm();
    this.listenToChanges();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({
      'user': [
        null,
        [Validators.required]
      ],
      'generate_key': [
        true
      ],
      'access_key': [
        null,
        [CdValidators.requiredIf({'generate_key': false})]
      ],
      'secret_key': [
        null,
        [CdValidators.requiredIf({'generate_key': false})]
      ]
    });
  }

  listenToChanges() {
    // Reset the validation status of various controls, especially those that are using
    // the 'requiredIf' validator. This is necessary because the controls itself are not
    // validated again if the status of their prerequisites have been changed.
    this.formGroup.get('generate_key').valueChanges.subscribe(() => {
      ['access_key', 'secret_key'].forEach((path) => {
        this.formGroup.get(path).updateValueAndValidity({onlySelf: true});
      });
    });
  }

  /**
   * Set the 'viewing' flag. If set to TRUE, the modal dialog is in 'View' mode,
   * otherwise in 'Add' mode. According to the mode the dialog and its controls
   * behave different.
   * @param {boolean} viewing
   */
  setViewing(viewing: boolean = true) {
    this.viewing = viewing;
  }

  /**
   * Set the values displayed in the dialog.
   */
  setValues(user: string, access_key: string, secret_key: string) {
    this.formGroup.setValue({
      'user': user,
      'generate_key': _.isEmpty(access_key),
      'access_key': access_key,
      'secret_key': secret_key
    });
  }

  /**
   * Set the user candidates displayed in the 'Username' dropdown box.
   */
  setUserCandidates(candidates: string[]) {
    this.userCandidates = candidates;
  }

  onSubmit() {
    const key: RgwUserS3Key = this.formGroup.value;
    this.submitAction.emit(key);
    this.bsModalRef.hide();
  }
}
