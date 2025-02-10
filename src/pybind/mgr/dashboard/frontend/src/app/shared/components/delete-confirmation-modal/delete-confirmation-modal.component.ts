import { Component, Inject, OnInit, Optional, TemplateRef, ViewChild } from '@angular/core';
import { UntypedFormControl, AbstractControl, ValidationErrors, Validators } from '@angular/forms';
import { Observable } from 'rxjs';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { BaseModal } from 'carbon-components-angular';
import { CdValidators } from '../../forms/cd-validators';
import { DeletionImpact } from '../../enum/delete-confirmation-modal-impact.enum';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './delete-confirmation-modal.component.html',
  styleUrls: ['./delete-confirmation-modal.component.scss']
})
export class DeleteConfirmationModalComponent extends BaseModal implements OnInit {
  @ViewChild(SubmitButtonComponent, { static: true })
  submitButton: SubmitButtonComponent;
  deletionForm: CdFormGroup;
  impactEnum = DeletionImpact;
  childFormGroup: CdFormGroup;
  childFormGroupTemplate: TemplateRef<any>;

  constructor(
    @Optional() @Inject('impact') public impact: DeletionImpact,
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
    this.impact = this.impact || DeletionImpact.medium;
  }

  ngOnInit() {
    const controls = {
      impact: new UntypedFormControl(this.impact),
      confirmation: new UntypedFormControl(false, {
        validators: [
          CdValidators.composeIf(
            {
              impact: DeletionImpact.medium
            },
            [Validators.requiredTrue]
          )
        ]
      }),
      confirmInput: new UntypedFormControl('', [
        CdValidators.composeIf({ impact: this.impactEnum.high }, [
          this.matchResourceName.bind(this),
          Validators.required
        ])
      ])
    };
    if (this.childFormGroup) {
      controls['child'] = this.childFormGroup;
    }
    this.deletionForm = new CdFormGroup(controls);
    if (!(this.submitAction || this.submitActionObservable)) {
      throw new Error('No submit action defined');
    }
  }

  matchResourceName(control: AbstractControl): ValidationErrors | null {
    if (this.itemNames && control.value !== String(this.itemNames?.[0])) {
      return { matchResource: true };
    }
    return null;
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
