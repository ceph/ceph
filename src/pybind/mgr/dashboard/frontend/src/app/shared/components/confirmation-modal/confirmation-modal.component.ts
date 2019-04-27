import { Component, OnInit, TemplateRef } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

@Component({
  selector: 'cd-confirmation-modal',
  templateUrl: './confirmation-modal.component.html',
  styleUrls: ['./confirmation-modal.component.scss']
})
export class ConfirmationModalComponent implements OnInit {
  bodyData: object;
  bodyTpl: TemplateRef<any>;
  buttonText: string;
  onSubmit: Function;
  onCancel: Function;
  titleText: string;

  bodyContext: object;
  confirmationForm: FormGroup;

  boundCancel = this.cancel.bind(this);

  constructor(public modalRef: BsModalRef) {
    this.confirmationForm = new FormGroup({});
  }

  ngOnInit() {
    this.bodyContext = this.bodyContext || {};
    this.bodyContext['$implicit'] = this.bodyData;
  }

  cancel() {
    this.modalRef.hide();
    if (this.onCancel) {
      this.onCancel();
    }
  }

  stopLoadingSpinner() {
    this.confirmationForm.setErrors({ cdSubmitButton: true });
  }
}
