import { Component, Inject, OnDestroy, OnInit, Optional, TemplateRef } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';

import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-confirmation-modal',
  templateUrl: './confirmation-modal.component.html',
  styleUrls: ['./confirmation-modal.component.scss'],
  providers: [
    { provide: 'warning', useValue: false },
    { provide: 'showSubmit', useValue: true },
    { provide: 'showCancel', useValue: true }
  ]
})
export class ConfirmationModalComponent extends BaseModal implements OnInit, OnDestroy {
  // Component only
  confirmationForm: UntypedFormGroup;
  private canceled = false;

  constructor(
    @Optional() @Inject('titleText') public titleText: string,
    @Optional() @Inject('buttonText') public buttonText: string,
    @Optional() @Inject('onSubmit') public onSubmit: Function,

    // One of them is needed
    @Optional() @Inject('bodyTpl') public bodyTpl?: TemplateRef<any>,
    @Optional() @Inject('description') public description?: TemplateRef<any>,

    // Optional
    @Optional() @Inject('warning') public warning = false,
    @Optional() @Inject('bodyData') public bodyData?: object,
    @Optional() @Inject('onCancel') public onCancel?: Function,
    @Optional() @Inject('bodyContext') public bodyContext?: object,
    @Optional() @Inject('showSubmit') public showSubmit = true,
    @Optional() @Inject('showCancel') public showCancel = true
  ) {
    super();
    this.confirmationForm = new UntypedFormGroup({});
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

  stopLoadingSpinner() {
    this.confirmationForm.setErrors({ cdSubmitButton: true });
  }
}
