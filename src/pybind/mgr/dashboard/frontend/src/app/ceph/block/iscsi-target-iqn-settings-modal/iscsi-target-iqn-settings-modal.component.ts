import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';

@Component({
  selector: 'cd-iscsi-target-iqn-settings-modal',
  templateUrl: './iscsi-target-iqn-settings-modal.component.html',
  styleUrls: ['./iscsi-target-iqn-settings-modal.component.scss']
})
export class IscsiTargetIqnSettingsModalComponent implements OnInit {
  target_controls: FormControl;
  target_default_controls: any;
  target_controls_limits: any;

  settingsForm: CdFormGroup;
  helpText: any;

  constructor(public modalRef: BsModalRef, public iscsiService: IscsiService) {}

  ngOnInit() {
    const fg = {};
    this.helpText = this.iscsiService.targetAdvancedSettings;

    _.forIn(this.target_default_controls, (_value, key) => {
      const validators = [];
      if (this.target_controls_limits && key in this.target_controls_limits) {
        if ('min' in this.target_controls_limits[key]) {
          validators.push(Validators.min(this.target_controls_limits[key]['min']));
        }
        if ('max' in this.target_controls_limits[key]) {
          validators.push(Validators.max(this.target_controls_limits[key]['max']));
        }
      }
      fg[key] = new FormControl(this.target_controls.value[key], { validators: validators });
    });

    this.settingsForm = new CdFormGroup(fg);
  }

  save() {
    const settings = {};
    _.forIn(this.settingsForm.controls, (control, key) => {
      if (!(control.value === '' || control.value === null)) {
        settings[key] = control.value;
      }
    });

    this.target_controls.setValue(settings);
    this.modalRef.hide();
  }

  isRadio(control) {
    return ['Yes', 'No'].indexOf(this.target_default_controls[control]) !== -1;
  }
}
