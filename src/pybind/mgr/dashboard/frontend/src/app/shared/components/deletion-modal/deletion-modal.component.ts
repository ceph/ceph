import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';
import { Observable } from 'rxjs';

import { CdFormGroup } from '../../forms/cd-form-group';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';

@Component({
  selector: 'cd-deletion-modal',
  templateUrl: './deletion-modal.component.html',
  styleUrls: ['./deletion-modal.component.scss']
})
export class DeletionModalComponent implements OnInit {
  @ViewChild(SubmitButtonComponent)
  submitButton: SubmitButtonComponent;
  description: TemplateRef<any>;
  metaType: string;
  deletionObserver: () => Observable<any>;
  deletionMethod: Function;
  modalRef: BsModalRef;

  deletionForm: CdFormGroup;

  // Parameters are destructed here than assigned to specific types and marked as optional
  setUp({
    modalRef,
    metaType,
    deletionMethod,
    deletionObserver,
    description
  }: {
    modalRef: BsModalRef;
    metaType: string;
    deletionMethod?: Function;
    deletionObserver?: () => Observable<any>;
    description?: TemplateRef<any>;
  }) {
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
    this.deletionObserver = deletionObserver;
    this.description = description;
  }

  ngOnInit() {
    this.deletionForm = new CdFormGroup({
      confirmation: new FormControl(false, [Validators.requiredTrue])
    });
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
    this.deletionForm.setErrors({ cdSubmitButton: true });
  }
}
