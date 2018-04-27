import { Component } from '@angular/core';

import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';

@Component({
  selector: 'cd-rgw-user-swift-key-modal',
  templateUrl: './rgw-user-swift-key-modal.component.html',
  styleUrls: ['./rgw-user-swift-key-modal.component.scss']
})
export class RgwUserSwiftKeyModalComponent {

  user: string;
  secret_key: string;

  constructor(public bsModalRef: BsModalRef) {}

  /**
   * Set the values displayed in the dialog.
   */
  setValues(user: string, secret_key: string) {
    this.user = user;
    this.secret_key = secret_key;
  }
}
