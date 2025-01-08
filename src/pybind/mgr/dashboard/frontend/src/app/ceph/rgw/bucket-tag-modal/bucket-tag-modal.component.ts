import { Component, EventEmitter, Output } from '@angular/core';
import { Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';

@Component({
  selector: 'cd-bucket-tag-modal',
  templateUrl: './bucket-tag-modal.component.html',
  styleUrls: ['./bucket-tag-modal.component.scss']
})
export class BucketTagModalComponent {
  @Output()
  submitAction = new EventEmitter();

  form: CdFormGroup;
  editMode = false;
  currentKeyTags: string[];
  storedKey: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n
  ) {
    this.createForm();
  }

  private createForm() {
    this.form = this.formBuilder.group({
      key: [
        null,
        [
          Validators.required,
          CdValidators.custom('unique', (value: string) => {
            if (_.isEmpty(value) && !this.currentKeyTags) {
              return false;
            }
            return this.storedKey !== value && this.currentKeyTags.includes(value);
          }),
          CdValidators.custom('maxLength', (value: string) => {
            if (_.isEmpty(value)) return false;
            return value.length > 128;
          })
        ]
      ],
      value: [
        null,
        [
          Validators.required,
          CdValidators.custom('maxLength', (value: string) => {
            if (_.isEmpty(value)) return false;
            return value.length > 128;
          })
        ]
      ]
    });
  }

  onSubmit() {
    this.submitAction.emit(this.form.value);
    this.activeModal.close();
  }

  getMode() {
    return this.editMode ? this.actionLabels.EDIT : this.actionLabels.ADD;
  }

  fillForm(tag: Record<string, string>) {
    this.form.setValue(tag);
  }
}
