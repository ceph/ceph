import { Injectable } from '@angular/core';
import { ModalService } from 'carbon-components-angular';

@Injectable({
  providedIn: 'root'
})
export class ModalCdsService {
  modalRef: any;

  constructor(private modalService: ModalService) {}

  show(component: any, inputs = {}) {
    const createModal = this.modalService.create({
      component: component,
      inputs: inputs
    });
    this.modalRef = createModal.injector.get<any>(component);
    return this.modalRef;
  }

  hasOpenModals() {
    return this.modalService.placeholderService.hasComponentRef;
  }

  dismissAll() {
    this.modalService.destroy();
  }

  stopLoadingSpinner(form: any) {
    this.modalRef[form].setErrors({ cdSubmitButton: true });
  }
}
