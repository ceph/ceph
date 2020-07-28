import { Component, EventEmitter, Output } from '@angular/core';
import { Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';

import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { RgwUserCapabilities } from '../models/rgw-user-capabilities';
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
  @Output()
  submitAction = new EventEmitter();

  formGroup: CdFormGroup;
  editing = true;
  types: string[] = [];
  resource: string;
  action: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n
  ) {
    this.resource = $localize`capability`;
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({
      type: [null, [Validators.required]],
      perm: [null, [Validators.required]]
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
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.ADD;
  }

  /**
   * Set the values displayed in the dialog.
   */
  setValues(type: string, perm: string) {
    this.formGroup.setValue({
      type: type,
      perm: perm
    });
  }

  /**
   * Set the current capabilities of the user.
   */
  setCapabilities(capabilities: RgwUserCapability[]) {
    // Parse the configured capabilities to get a list of types that
    // should be displayed.
    const usedTypes: string[] = [];
    capabilities.forEach((capability) => {
      usedTypes.push(capability.type);
    });
    this.types = [];
    RgwUserCapabilities.getAll().forEach((type) => {
      if (_.indexOf(usedTypes, type) === -1) {
        this.types.push(type);
      }
    });
  }

  onSubmit() {
    const capability: RgwUserCapability = this.formGroup.value;
    this.submitAction.emit(capability);
    this.activeModal.close();
  }
}
