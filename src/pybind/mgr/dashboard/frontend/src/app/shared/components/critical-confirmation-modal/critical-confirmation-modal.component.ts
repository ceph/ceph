import { Component, Inject, OnInit, Optional, TemplateRef, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { Observable } from 'rxjs';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './critical-confirmation-modal.component.html',
  styleUrls: ['./critical-confirmation-modal.component.scss']
})
export class CriticalConfirmationModalComponent extends BaseModal implements OnInit {
  @ViewChild(SubmitButtonComponent, { static: true })
  submitButton: SubmitButtonComponent;
  deletionForm: CdFormGroup;

  childFormGroup: CdFormGroup;
  childFormGroupTemplate: TemplateRef<any>;

  constructor(
    @Optional() @Inject('itemDescription') public itemDescription: 'entry',
    @Optional() @Inject('itemNames') public itemNames: string[],
    @Optional() @Inject('actionDescription') public actionDescription = 'delete',
    @Optional() @Inject('submitAction') public submitAction?: Function,
    @Optional() @Inject('backAction') public backAction?: Function,
    @Optional() @Inject('bodyTemplate') public bodyTemplate?: TemplateRef<any>,
    @Optional() @Inject('bodyContext') public bodyContext?: object,
    @Optional() @Inject('infoMessage') public infoMessage?: string,
    @Optional()
    @Inject('submitActionObservable')
    public submitActionObservable?: () => Observable<any>,
    @Optional()
    @Inject('callBackAtionObservable')
    public callBackAtionObservable?: () => Observable<any>
  ) {
    super();
    this.actionDescription = actionDescription || 'delete';
  }

  ngOnInit() {
    const controls = {
      confirmation: new UntypedFormControl(false, [Validators.requiredTrue])
    };
    if (this.childFormGroup) {
      controls['child'] = this.childFormGroup;
    }
    this.deletionForm = new CdFormGroup(controls);
    if (!(this.submitAction || this.submitActionObservable)) {
      throw new Error('No submit action defined');
    }
  }

  callSubmitAction() {
    if (this.submitActionObservable) {
      this.submitActionObservable().subscribe({
        error: this.stopLoadingSpinner.bind(this),
        complete: this.hideModal.bind(this)
      });
    } else {
      this.submitAction();
    }
  }

  callBackAction() {
    if (this.callBackAtionObservable) {
      this.callBackAtionObservable().subscribe({
        error: this.stopLoadingSpinner.bind(this),
        complete: this.hideModal.bind(this)
      });
    } else {
      this.backAction();
    }
  }

  hideModal() {
    this.closeModal();
  }

  stopLoadingSpinner() {
    this.deletionForm.setErrors({ cdSubmitButton: true });
  }
}
