import { Component, Inject, OnInit, Optional } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';

import { BaseModal } from 'carbon-components-angular';
import _ from 'lodash';

import { IscsiService } from '~/app/shared/api/iscsi.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-iscsi-target-iqn-settings-modal',
  templateUrl: './iscsi-target-iqn-settings-modal.component.html',
  styleUrls: ['./iscsi-target-iqn-settings-modal.component.scss']
})
export class IscsiTargetIqnSettingsModalComponent extends BaseModal implements OnInit {
  settingsForm: CdFormGroup;

  constructor(
    public iscsiService: IscsiService,
    public actionLabels: ActionLabelsI18n,

    @Optional() @Inject('target_controls') public target_controls: UntypedFormControl,
    @Optional() @Inject('target_default_controls') public target_default_controls: any,
    @Optional() @Inject('target_controls_limits') public target_controls_limits: any
  ) {
    super();
  }

  ngOnInit() {
    const fg: Record<string, UntypedFormControl> = {};
    _.forIn(this.target_default_controls, (_value, key) => {
      fg[key] = new UntypedFormControl(this.target_controls.value[key]);
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
    this.closeModal();
  }

  getTargetControlLimits(setting: string) {
    if (this.target_controls_limits) {
      return this.target_controls_limits[setting];
    }
    // backward compatibility
    if (['Yes', 'No'].includes(this.target_default_controls[setting])) {
      return { type: 'bool' };
    }
    return { type: 'int' };
  }
}
