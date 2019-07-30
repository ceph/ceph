import { Component, OnInit, TemplateRef } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

@Component({
  selector: 'cd-confirmation-modal',
  templateUrl: './confirmation-modal.component.html',
  styleUrls: ['./confirmation-modal.component.scss']
})
export class ConfirmationModalComponent implements OnInit, OnDestroy {
  // Needed
  bodyTpl: TemplateRef<any>;
  buttonText: string;
  titleText: string;
  onSubmit: Function;

  // Optional
  bodyData?: object;
  onCancel?: Function;
  bodyContext?: object;

  // Component only
  boundCancel = this.cancel.bind(this);
  confirmationForm: FormGroup;

  constructor(public modalRef: BsModalRef) {
    this.confirmationForm = new FormGroup({});
  }

  ngOnInit() {
    this.bodyContext = this.bodyContext || {};
    this.bodyContext['$implicit'] = this.bodyData;
    if (!this.onSubmit) {
      throw new Error('No submit action defined');
    } else if (!this.buttonText) {
      throw new Error('No action name defined');
    } else if (!this.titleText) {
      throw new Error('No title defined');
    } else if (!this.bodyTpl) {
      throw new Error('No description defined');
    }
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
