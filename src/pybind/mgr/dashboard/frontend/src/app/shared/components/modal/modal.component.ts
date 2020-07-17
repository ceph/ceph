import { Component, EventEmitter, Input, Output } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'cd-modal',
  templateUrl: './modal.component.html',
  styleUrls: ['./modal.component.scss']
})
export class ModalComponent {
  @Input()
  modalRef: NgbActiveModal;

  /**
   * Should be a function that is triggered when the modal is hidden.
   */
  @Output()
  hide = new EventEmitter();

  close() {
    this.modalRef?.close();
    this.hide.emit();
  }
}
