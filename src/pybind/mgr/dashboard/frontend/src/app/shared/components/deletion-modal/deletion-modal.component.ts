import {
  Component, OnInit, TemplateRef, ViewChild
} from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { SubmitButtonComponent } from '../submit-button/submit-button.component';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './deletion-modal.component.html',
  styleUrls: ['./deletion-modal.component.scss']
})
export class DeletionModalComponent implements OnInit {
  @ViewChild(SubmitButtonComponent) submitButton: SubmitButtonComponent;
  description: TemplateRef<any>;
  metaType: string;
  pattern = 'yes';
  deletionObserver: () => Observable<any>;
  deletionMethod: Function;
  modalRef: BsModalRef;

  deletionForm: FormGroup;
  confirmation: FormControl;

  // Parameters are destructed here than assigned to specific types and marked as optional
  setUp({modalRef, metaType, deletionMethod, pattern, deletionObserver, description}:
          { modalRef: BsModalRef, metaType: string, deletionMethod?: Function, pattern?: string,
            deletionObserver?: () => Observable<any>, description?: TemplateRef<any>}) {
    if (!modalRef) {
      throw new Error('No modal reference');
    } else if (!metaType) {
      throw new Error('No meta type');
    } else if (!(deletionMethod || deletionObserver)) {
      throw new Error('No deletion method');
    }
    this.metaType = metaType;
    this.modalRef = modalRef;
    this.deletionMethod = deletionMethod;
    this.pattern = pattern || this.pattern;
    this.deletionObserver = deletionObserver;
    this.description = description;
  }

  ngOnInit() {
    this.confirmation = new FormControl('', {
      validators: [
        Validators.required
      ],
      updateOn: 'blur'
    });
    this.deletionForm = new FormGroup({
      confirmation: this.confirmation
    });
  }

  invalidControl(submitted: boolean, error?: string): boolean {
    const control = this.confirmation;
    return !!(
      (submitted || control.dirty) &&
      control.invalid &&
      (error ? control.errors[error] : true)
    );
  }

  updateConfirmation($e) {
    if ($e.key !== 'Enter') {
      return;
    }
    this.confirmation.setValue($e.target.value);
    this.confirmation.markAsDirty();
    this.confirmation.updateValueAndValidity();
  }

  deletionCall() {
    if (this.deletionObserver) {
      this.deletionObserver().subscribe(
        undefined,
        this.stopLoadingSpinner.bind(this),
        this.hideModal.bind(this)
      );
    } else {
      this.deletionMethod();
    }
  }

  hideModal() {
    this.modalRef.hide();
  }

  stopLoadingSpinner() {
    this.deletionForm.setErrors({'cdSubmitButton': true});
  }

  escapeRegExp(text) {
    return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}
