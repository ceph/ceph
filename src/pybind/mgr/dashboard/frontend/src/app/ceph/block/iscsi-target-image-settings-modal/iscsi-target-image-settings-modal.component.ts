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
  disk_controls_limits: any;
  backstores: any;

  settingsForm: CdFormGroup;

  constructor(public modalRef: BsModalRef, public iscsiService: IscsiService) {}

  ngOnInit() {
    const fg = {
      backstore: new FormControl(this.imagesSettings[this.image]['backstore'])
    };
    _.forEach(this.backstores, (backstore) => {
      const model = this.imagesSettings[this.image][backstore] || {};
      _.forIn(this.disk_default_controls[backstore], (_value, key) => {
        fg[key] = new FormControl(model[key]);
      });
    });

    this.settingsForm = new CdFormGroup(fg);
  }

  getDiskControlLimits(backstore, setting) {
    return this.disk_controls_limits[backstore][setting];
  }

  save() {
    const backstore = this.settingsForm.controls['backstore'].value;
    const settings = {};
    _.forIn(this.settingsForm.controls, (control, key) => {
      if (
        !(control.value === '' || control.value === null) &&
        key in this.disk_default_controls[this.settingsForm.value['backstore']]
      ) {
        settings[key] = control.value;
        // If one setting belongs to multiple backstores, we have to update it in all backstores
        _.forEach(this.backstores, (currentBackstore) => {
          if (currentBackstore !== backstore) {
            const model = this.imagesSettings[this.image][currentBackstore] || {};
            if (key in model) {
              this.imagesSettings[this.image][currentBackstore][key] = control.value;
            }
          }
        });
      }
    });
    this.imagesSettings[this.image]['backstore'] = backstore;
    this.imagesSettings[this.image][backstore] = settings;
    this.imagesSettings = { ...this.imagesSettings };
    this.modalRef.hide();
  }
}
