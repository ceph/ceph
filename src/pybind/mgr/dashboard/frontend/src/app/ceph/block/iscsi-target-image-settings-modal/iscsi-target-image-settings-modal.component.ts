import { Component, OnInit } from '@angular/core';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { IscsiService } from '../../../shared/api/iscsi.service';

@Component({
  selector: 'cd-iscsi-target-image-settings-modal',
  templateUrl: './iscsi-target-image-settings-modal.component.html',
  styleUrls: ['./iscsi-target-image-settings-modal.component.scss']
})
export class IscsiTargetImageSettingsModalComponent implements OnInit {
  image: string;
  imagesSettings: any;
  disk_default_controls: any;
  backstores: any;

  model: any;
  helpText: any;

  constructor(public modalRef: BsModalRef, public iscsiService: IscsiService) {}

  ngOnInit() {
    this.helpText = this.iscsiService.imageAdvancedSettings;

    this.model = _.cloneDeep(this.imagesSettings[this.image]);
    _.forEach(this.backstores, (backstore) => {
      this.model[backstore] = this.model[backstore] || {};
    });
  }

  save() {
    const backstore = this.model.backstore;
    const settings = {};
    _.forIn(this.model[backstore], (value, key) => {
      if (!(value === '' || value === null)) {
        settings[key] = value;
      }
    });
    this.imagesSettings[this.image]['backstore'] = backstore;
    this.imagesSettings[this.image][backstore] = settings;
    this.imagesSettings = { ...this.imagesSettings };
    this.modalRef.hide();
  }
}
