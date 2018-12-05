import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalService } from 'ngx-bootstrap/modal';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserSwiftKey } from '../models/rgw-user-swift-key';
import { RgwUserS3KeyModalComponent } from '../rgw-user-s3-key-modal/rgw-user-s3-key-modal.component';
import { RgwUserSwiftKeyModalComponent } from '../rgw-user-swift-key-modal/rgw-user-swift-key-modal.component';

@Component({
  selector: 'cd-rgw-user-details',
  templateUrl: './rgw-user-details.component.html',
  styleUrls: ['./rgw-user-details.component.scss']
})
export class RgwUserDetailsComponent implements OnChanges, OnInit {
  @ViewChild('accessKeyTpl')
  public accessKeyTpl: TemplateRef<any>;
  @ViewChild('secretKeyTpl')
  public secretKeyTpl: TemplateRef<any>;

  @Input()
  selection: CdTableSelection;

  // Details tab
  user: any;

  // Keys tab
  keys: any = [];
  keysColumns: CdTableColumn[] = [];
  keysSelection: CdTableSelection = new CdTableSelection();

  constructor(
    private rgwUserService: RgwUserService,
    private bsModalService: BsModalService,
    private i18n: I18n
  ) {}

  ngOnInit() {
    this.keysColumns = [
      {
        name: this.i18n('Username'),
        prop: 'username',
        flexGrow: 1
      },
      {
        name: this.i18n('Type'),
        prop: 'type',
        flexGrow: 1
      }
    ];
  }

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.user = this.selection.first();

      // Sort subusers and capabilities.
      this.user.subusers = _.sortBy(this.user.subusers, 'id');
      this.user.caps = _.sortBy(this.user.caps, 'type');

      // Load the user/bucket quota of the selected user.
      this.rgwUserService.getQuota(this.user.uid).subscribe((resp: object) => {
        _.extend(this.user, resp);
      });

      // Process the keys.
      this.keys = [];
      this.user.keys.forEach((key: RgwUserS3Key) => {
        this.keys.push({
          id: this.keys.length + 1, // Create an unique identifier
          type: 'S3',
          username: key.user,
          ref: key
        });
      });
      this.user.swift_keys.forEach((key: RgwUserSwiftKey) => {
        this.keys.push({
          id: this.keys.length + 1, // Create an unique identifier
          type: 'Swift',
          username: key.user,
          ref: key
        });
      });
      this.keys = _.sortBy(this.keys, 'user');
    }
  }

  updateKeysSelection(selection: CdTableSelection) {
    this.keysSelection = selection;
  }

  showKeyModal() {
    const key = this.keysSelection.first();
    const modalRef = this.bsModalService.show(
      key.type === 'S3' ? RgwUserS3KeyModalComponent : RgwUserSwiftKeyModalComponent
    );
    switch (key.type) {
      case 'S3':
        modalRef.content.setViewing();
        modalRef.content.setValues(key.ref.user, key.ref.access_key, key.ref.secret_key);
        break;
      case 'Swift':
        modalRef.content.setValues(key.ref.user, key.ref.secret_key);
        break;
    }
  }
}
