import { Component, OnDestroy, OnInit, TemplateRef } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { Subscription } from 'rxjs';

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
  private onHide: Subscription;
  private canceled = false;

  constructor(public modalRef: BsModalRef, private modalService: BsModalService) {
    this.confirmationForm = new FormGroup({});
    this.onHide = this.modalService.onHide.subscribe((e) => {
      if (this.onCancel && (e || this.canceled)) {
        this.onCancel();
      }
    });
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
    this.onHide.unsubscribe();
  }

  cancel() {
    this.canceled = true;
    this.modalRef.hide();
  }

  stopLoadingSpinner() {
    this.confirmationForm.setErrors({ cdSubmitButton: true });
  }
}
