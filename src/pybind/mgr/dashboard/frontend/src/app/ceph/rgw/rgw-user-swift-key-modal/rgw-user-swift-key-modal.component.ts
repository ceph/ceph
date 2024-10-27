import { Component } from '@angular/core';

import { BaseModal } from 'carbon-components-angular';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-rgw-user-swift-key-modal',
  templateUrl: './rgw-user-swift-key-modal.component.html',
  styleUrls: ['./rgw-user-swift-key-modal.component.scss']
})
export class RgwUserSwiftKeyModalComponent extends BaseModal {
  user: string;
  secret_key: string;
  resource: string;
  action: string;

  constructor(public actionLabels: ActionLabelsI18n) {
    super();
    this.resource = $localize`Swift Key`;
    this.action = this.actionLabels.SHOW;
  }

  /**
   * Set the values displayed in the dialog.
   */
  setValues(user: string, secret_key: string) {
    this.user = user;
    this.secret_key = secret_key;
  }
}
