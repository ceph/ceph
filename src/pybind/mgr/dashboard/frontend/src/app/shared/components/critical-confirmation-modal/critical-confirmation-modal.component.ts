import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { Observable } from 'rxjs';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
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
  callBackAtionObservable: () => Observable<any>;
  submitAction: Function;
  backAction: Function;
  deletionForm: CdFormGroup;
  itemDescription: 'entry';
  itemNames: { poolName: string, namespace: string, imageName: string }[] = [];
  actionDescription = 'delete';
  infoMessage: string;

  childFormGroup: CdFormGroup;
  childFormGroupTemplate: TemplateRef<any>;

  providedName: string;

  constructor(public activeModal: NgbActiveModal) {}

  ngOnInit() {
    this.providedName = '';

    const controls = {
      confirmationInput: new UntypedFormControl('', [
        Validators.required,
        this.confirmationValidator.bind(this)
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

  git clone --depth 1 --single-branch git@github.com:ceph/ceph.git
  git clone --depth 1 --single-branch https://github.com/himanshu-0611/ceph-dev.git
  
  confirmationValidator(control: UntypedFormControl) {
    const input = control.value;
    this.providedName = input;
  
    let resourceName = 'default';
  
    if (this.itemNames.length > 0) {
      const item = this.itemNames[0];
  
      if (typeof item === 'string') {
        resourceName = item;
      } else if (item.poolName && item.imageName) {
        resourceName = `${item.poolName}/${item.imageName}`;
      } else if (item.poolName) {
        resourceName = item.poolName;
      } else if (item.imageName) {
        resourceName = item.imageName;
      }
    }
  
    return input === resourceName ? null : { mismatch: true };
  }

  onInputChange() {
    this.deletionForm.controls.confirmationInput.updateValueAndValidity();
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
