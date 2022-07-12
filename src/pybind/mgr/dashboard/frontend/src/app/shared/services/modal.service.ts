import { Injectable } from '@angular/core';

import { NgbModal, NgbModalOptions, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

@Injectable({
  providedIn: 'root'
})
export class ModalService {
  constructor(private modal: NgbModal) {}

  show(component: any, initialState?: any, options?: NgbModalOptions): NgbModalRef {
    const modalRef = this.modal.open(component, options);

    if (initialState) {
      Object.assign(modalRef.componentInstance, initialState);
    }

    return modalRef;
  }

  dismissAll() {
    this.modal.dismissAll();
  }

  hasOpenModals() {
    return this.modal.hasOpenModals();
  }
}
