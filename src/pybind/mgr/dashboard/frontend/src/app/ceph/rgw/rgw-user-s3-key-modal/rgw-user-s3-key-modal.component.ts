import { Component, EventEmitter, Output } from '@angular/core';
import { Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
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
  @Output()
  submitAction = new EventEmitter();

  formGroup: CdFormGroup;
  viewing = true;
  userCandidates: string[] = [];
  resource: string;
  action: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    this.resource = this.i18n('S3 Key');
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({
      user: [null, [Validators.required]],
      generate_key: [true],
      access_key: [null, [CdValidators.requiredIf({ generate_key: false })]],
      secret_key: [null, [CdValidators.requiredIf({ generate_key: false })]]
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
    this.action = this.viewing ? this.actionLabels.SHOW : this.actionLabels.CREATE;
  }

  /**
   * Set the values displayed in the dialog.
   */
  setValues(user: string, access_key: string, secret_key: string) {
    this.formGroup.setValue({
      user: user,
      generate_key: _.isEmpty(access_key),
      access_key: access_key,
      secret_key: secret_key
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
    this.activeModal.close();
  }
}
