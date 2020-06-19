import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';
import { Observable } from 'rxjs';

import { CdFormGroup } from '../../forms/cd-form-group';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './critical-confirmation-modal.component.html',
  styleUrls: ['./critical-confirmation-modal.component.scss']
})
export class CriticalConfirmationModalComponent implements OnInit {
  @ViewChild(SubmitButtonComponent, { static: true })
  submitButton: SubmitButtonComponent;
  bodyTemplate: TemplateRef<any>;
  bodyContext: object;
  submitActionObservable: () => Observable<any>;
  submitAction: Function;
  deletionForm: CdFormGroup;
  itemDescription: 'entry';
  itemNames: string[];
  actionDescription = 'delete';

  constructor(public modalRef: BsModalRef) {}

  ngOnInit() {
    this.deletionForm = new CdFormGroup({
      confirmation: new FormControl(false, [Validators.requiredTrue])
    });

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

  hideModal() {
    this.modalRef.hide();
  }

  stopLoadingSpinner() {
    this.deletionForm.setErrors({ cdSubmitButton: true });
  }
}
