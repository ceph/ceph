import { Component } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { I18n } from '@ngx-translate/i18n-polyfill';

import { ActionLabelsI18n } from '../../../shared/constants/app.constants';

@Component({
  selector: 'cd-rgw-user-swift-key-modal',
  templateUrl: './rgw-user-swift-key-modal.component.html',
  styleUrls: ['./rgw-user-swift-key-modal.component.scss']
})
export class RgwUserSwiftKeyModalComponent {
  user: string;
  secret_key: string;
  resource: string;
  action: string;

  constructor(
    public activeModal: NgbActiveModal,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    this.resource = this.i18n('Swift Key');
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
