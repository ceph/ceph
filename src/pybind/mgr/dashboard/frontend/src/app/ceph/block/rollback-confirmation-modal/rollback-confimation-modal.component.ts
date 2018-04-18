import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs/Subject';

@Component({
  selector: 'cd-rollback-confimation-modal',
  templateUrl: './rollback-confimation-modal.component.html',
  styleUrls: ['./rollback-confimation-modal.component.scss']
})
export class RollbackConfirmationModalComponent implements OnInit {

  snapName: string;

  rollbackForm: FormGroup;

  public onSubmit: Subject<string>;

  constructor(public modalRef: BsModalRef) {
    this.createForm();
  }

  createForm() {
    this.rollbackForm = new FormGroup({});
  }

  ngOnInit() {
    this.onSubmit = new Subject();
  }

  submit() {
    this.onSubmit.next(this.snapName);
  }

  stopLoadingSpinner() {
    this.rollbackForm.setErrors({'cdSubmitButton': true});
  }
}
