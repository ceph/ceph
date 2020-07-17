import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import * as _ from 'lodash';

import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { ModalService } from '../../../shared/services/modal.service';
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
  selection: any;

  // Details tab
  user: any;
  maxBucketsMap: {};

  // Keys tab
  keys: any = [];
  keysColumns: CdTableColumn[] = [];
  keysSelection: CdTableSelection = new CdTableSelection();

  icons = Icons;

  constructor(private rgwUserService: RgwUserService, private modalService: ModalService) {}

  ngOnInit() {
    this.keysColumns = [
      {
        name: $localize`Username`,
        prop: 'username',
        flexGrow: 1
      },
      {
        name: $localize`Type`,
        prop: 'type',
        flexGrow: 1
      }
    ];
    this.maxBucketsMap = {
      '-1': $localize`Disabled`,
      0: $localize`Unlimited`
    };
  }

  ngOnChanges() {
    if (this.selection) {
      this.user = this.selection;

      // Sort subusers and capabilities.
      this.user.subusers = _.sortBy(this.user.subusers, 'id');
      this.user.caps = _.sortBy(this.user.caps, 'type');

      // Load the user/bucket quota of the selected user.
      this.rgwUserService.getQuota(this.user.uid).subscribe((resp: object) => {
        _.extend(this.user, resp);
      });

      // Process the keys.
      this.keys = [];
      if (this.user.keys) {
        this.user.keys.forEach((key: RgwUserS3Key) => {
          this.keys.push({
            id: this.keys.length + 1, // Create an unique identifier
            type: 'S3',
            username: key.user,
            ref: key
          });
        });
      }
      if (this.user.swift_keys) {
        this.user.swift_keys.forEach((key: RgwUserSwiftKey) => {
          this.keys.push({
            id: this.keys.length + 1, // Create an unique identifier
            type: 'Swift',
            username: key.user,
            ref: key
          });
        });
      }

      this.keys = _.sortBy(this.keys, 'user');
    }
  }

  updateKeysSelection(selection: CdTableSelection) {
    this.keysSelection = selection;
  }

  showKeyModal() {
    const key = this.keysSelection.first();
    const modalRef = this.modalService.show(
      key.type === 'S3' ? RgwUserS3KeyModalComponent : RgwUserSwiftKeyModalComponent
    );
    switch (key.type) {
      case 'S3':
        modalRef.componentInstance.setViewing();
        modalRef.componentInstance.setValues(key.ref.user, key.ref.access_key, key.ref.secret_key);
        break;
      case 'Swift':
        modalRef.componentInstance.setValues(key.ref.user, key.ref.secret_key);
        break;
    }
  }
}
