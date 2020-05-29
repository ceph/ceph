import { Component, OnInit } from '@angular/core';
import { AbstractControl, FormControl } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';

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
  api_version: number;
  disk_default_controls: any;
  disk_controls_limits: any;
  backstores: any;
  control: AbstractControl;

  settingsForm: CdFormGroup;

  constructor(public activeModal: NgbActiveModal, public iscsiService: IscsiService) {}

  ngOnInit() {
    const fg: Record<string, FormControl> = {
      backstore: new FormControl(this.imagesSettings[this.image]['backstore']),
      lun: new FormControl(this.imagesSettings[this.image]['lun']),
      wwn: new FormControl(this.imagesSettings[this.image]['wwn'])
    };
    _.forEach(this.backstores, (backstore) => {
      const model = this.imagesSettings[this.image][backstore] || {};
      _.forIn(this.disk_default_controls[backstore], (_value, key) => {
        fg[key] = new FormControl(model[key]);
      });
    });

    this.settingsForm = new CdFormGroup(fg);
  }

  getDiskControlLimits(backstore: string, setting: string) {
    if (this.disk_controls_limits) {
      return this.disk_controls_limits[backstore][setting];
    }
    // backward compatibility
    return { type: 'int' };
  }

  save() {
    const backstore = this.settingsForm.controls['backstore'].value;
    const lun = this.settingsForm.controls['lun'].value;
    const wwn = this.settingsForm.controls['wwn'].value;
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
    this.imagesSettings[this.image]['lun'] = lun;
    this.imagesSettings[this.image]['wwn'] = wwn;
    this.imagesSettings[this.image][backstore] = settings;
    this.imagesSettings = { ...this.imagesSettings };
    this.control.updateValueAndValidity({ emitEvent: false });
    this.activeModal.close();
  }
}
