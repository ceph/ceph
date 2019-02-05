import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { IscsiService } from '../../../shared/api/iscsi.service';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';

@Component({
  selector: 'cd-iscsi-target-image-settings-modal',
  templateUrl: './iscsi-target-image-settings-modal.component.html',
  styleUrls: ['./iscsi-target-image-settings-modal.component.scss']
})
export class IscsiTargetImageSettingsModalComponent implements OnInit {
  image: string;
  imagesSettings: any;
  disk_default_controls: any;

  settingsForm: CdFormGroup;
  helpText: any;

  constructor(public modalRef: BsModalRef, public iscsiService: IscsiService) {}

  ngOnInit() {
    const fg = {};
    const currentSettings = this.imagesSettings[this.image];
    this.helpText = this.iscsiService.imageAdvancedSettings;

    _.forIn(this.disk_default_controls, (value, key) => {
      fg[key] = new FormControl(currentSettings[key]);
    });

    this.settingsForm = new CdFormGroup(fg);
  }

  save() {
    const settings = {};
    _.forIn(this.settingsForm.value, (value, key) => {
      if (!(value === '' || value === null)) {
        settings[key] = value;
      }
    });

    this.imagesSettings[this.image] = settings;
    this.imagesSettings = { ...this.imagesSettings };
    this.modalRef.hide();
  }
}
