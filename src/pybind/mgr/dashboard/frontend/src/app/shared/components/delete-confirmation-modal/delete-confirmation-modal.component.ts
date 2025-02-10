import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { AbstractControl, UntypedFormControl, ValidationErrors, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable } from 'rxjs';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { CdValidators } from '../../forms/cd-validators';
import { DeletionImpact } from '../../enum/delete-confirmation-modal-impact.enum';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './delete-confirmation-modal.component.html',
  styleUrls: ['./delete-confirmation-modal.component.scss']
})
export class DeleteConfirmationModalComponent implements OnInit {
  @ViewChild(SubmitButtonComponent, { static: true })
  submitButton: SubmitButtonComponent;
  bodyTemplate: TemplateRef<any>;
  bodyContext: object;
  submitActionObservable: () => Observable<any>;
  callBackAtionObservable: () => Observable<any>;
  submitAction: Function;
  backAction: Function;
  deletionForm: CdFormGroup;
  itemDescription: 'entry';
  itemNames: string[];
  actionDescription = 'delete';
  impactEnum = DeletionImpact;
  childFormGroup: CdFormGroup;
  childFormGroupTemplate: TemplateRef<any>;
  impact: DeletionImpact;
  constructor(public activeModal: NgbActiveModal) {
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
    this.activeModal.close();
  }

  stopLoadingSpinner() {
    this.deletionForm.setErrors({ cdSubmitButton: true });
  }
}
