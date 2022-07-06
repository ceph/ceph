import { Component, EventEmitter, Input, Output } from '@angular/core';
import { Router } from '@angular/router';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'cd-modal',
  templateUrl: './modal.component.html',
  styleUrls: ['./modal.component.scss']
})
export class ModalComponent {
  @Input()
  modalRef: NgbActiveModal;
  @Input()
  pageURL: string;

  /**
   * Should be a function that is triggered when the modal is hidden.
   */
  @Output()
  hide = new EventEmitter();

  constructor(private router: Router) {}

  close() {
    this.pageURL
      ? this.router.navigate([this.pageURL, { outlets: { modal: null } }])
      : this.modalRef?.close();
    this.hide.emit();
  }
}
