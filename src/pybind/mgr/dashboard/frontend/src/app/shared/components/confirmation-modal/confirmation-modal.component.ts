import { Component, OnDestroy, OnInit, TemplateRef } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'cd-confirmation-modal',
  templateUrl: './confirmation-modal.component.html',
  styleUrls: ['./confirmation-modal.component.scss']
})
export class ConfirmationModalComponent implements OnInit, OnDestroy {
  // Needed
  buttonText: string;
  titleText: string;
  onSubmit: Function;

  // One of them is needed
  bodyTpl?: TemplateRef<any>;
  description?: TemplateRef<any>;

  // Optional
  bodyData?: object;
  onCancel?: Function;
  bodyContext?: object;

  // Component only
  boundCancel = this.cancel.bind(this);
  confirmationForm: FormGroup;
  private canceled = false;

  constructor(public activeModal: NgbActiveModal) {
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
    } else if (!this.bodyTpl && !this.description) {
      throw new Error('No description defined');
    }
  }

  ngOnDestroy() {
    if (this.onCancel && this.canceled) {
      this.onCancel();
    }
  }

  cancel() {
    this.canceled = true;
    this.activeModal.close();
  }

  stopLoadingSpinner() {
    this.confirmationForm.setErrors({ cdSubmitButton: true });
  }
}
