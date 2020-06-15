import { Component, EventEmitter, Input, Output } from '@angular/core';

import { BsModalRef } from 'ngx-bootstrap/modal';

@Component({
  selector: 'cd-modal',
  templateUrl: './modal.component.html',
  styleUrls: ['./modal.component.scss']
})
export class ModalComponent {
  @Input()
  modalRef: BsModalRef;

  /**
   * Should be a function that is triggered when the modal is hidden.
   */
  @Output()
  hide = new EventEmitter();

  close() {
    if (this.modalRef) {
      this.modalRef.hide();
    }
    this.hide.emit();
  }
}
