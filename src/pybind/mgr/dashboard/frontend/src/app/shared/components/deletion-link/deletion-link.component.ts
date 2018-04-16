import {
  Component, EventEmitter, Input, OnInit, Output, TemplateRef, ViewChild
} from '@angular/core';
import { FormControl, FormGroup, FormGroupDirective, Validators } from '@angular/forms';

import { BsModalRef, BsModalService } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { SubmitButtonComponent } from '../submit-button/submit-button.component';

@Component({
  selector: 'cd-deletion-link',
  templateUrl: './deletion-link.component.html',
  styleUrls: ['./deletion-link.component.scss']
})
export class DeletionLinkComponent implements OnInit {
  @ViewChild(SubmitButtonComponent) submitButton: SubmitButtonComponent;
  @Input() metaType: string;
  @Input() pattern = 'yes';
  @Input() deletionObserver: () => Observable<any>;
  @Output() toggleDeletion = new EventEmitter();
  bsModalRef: BsModalRef;
  deletionForm: FormGroup;
  confirmation: FormControl;
  delete: Function;

  constructor(public modalService: BsModalService) {}

  ngOnInit() {
    this.confirmation = new FormControl('', {
      validators: [
        Validators.required,
        Validators.pattern(this.pattern)
      ],
      updateOn: 'blur'
    });
    this.deletionForm = new FormGroup({
      confirmation: this.confirmation
    });
  }

  showModal(template: TemplateRef<any>) {
    this.deletionForm.reset();
    this.bsModalRef = this.modalService.show(template);
    this.delete = () => {
      this.submitButton.submit();
    };
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
        () => this.stopLoadingSpinner(),
        () => this.hideModal()
      );
    } else {
      this.toggleDeletion.emit();
    }
  }

  hideModal() {
    this.bsModalRef.hide();
  }

  stopLoadingSpinner() {
    this.submitButton.loading = false;
  }
}
