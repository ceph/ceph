import { Component, EventEmitter, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';

import { RgwUserCapability } from '../models/rgw-user-capability';

@Component({
  selector: 'cd-rgw-user-capability-modal',
  templateUrl: './rgw-user-capability-modal.component.html',
  styleUrls: ['./rgw-user-capability-modal.component.scss']
})
export class RgwUserCapabilityModalComponent {

  /**
   * The event that is triggered when the 'Add' or 'Update' button
   * has been pressed.
   */
  @Output() submitAction = new EventEmitter();

  formGroup: FormGroup;
  editing = true;
  types: string[] = [];

  constructor(private formBuilder: FormBuilder,
              public bsModalRef: BsModalRef) {
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({
      'type': [
        null,
        [Validators.required]
      ],
      'perm': [
        null,
        [Validators.required]
      ]
    });
  }

  /**
   * Set the 'editing' flag. If set to TRUE, the modal dialog is in 'Edit' mode,
   * otherwise in 'Add' mode. According to the mode the dialog and its controls
   * behave different.
   * @param {boolean} viewing
   */
  setEditing(editing: boolean = true) {
    this.editing = editing;
  }

  /**
   * Set the values displayed in the dialog.
   */
  setValues(type: string, perm: string) {
    this.formGroup.setValue({
      'type': type,
      'perm': perm
    });
  }

  /**
   * Set the current capabilities of the user.
   */
  setCapabilities(capabilities: RgwUserCapability[]) {
    // Parse the configured capabilities to get a list of types that
    // should be displayed.
    const usedTypes = [];
    capabilities.forEach((capability) => {
      usedTypes.push(capability.type);
    });
    this.types = [];
    ['users', 'buckets', 'metadata', 'usage', 'zone'].forEach((type) => {
      if (_.indexOf(usedTypes, type) === -1) {
        this.types.push(type);
      }
    });
  }

  onSubmit() {
    const capability: RgwUserCapability = this.formGroup.value;
    this.submitAction.emit(capability);
    this.bsModalRef.hide();
  }
}
