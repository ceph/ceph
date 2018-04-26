import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs/Subject';

@Component({
  selector: 'cd-flatten-confimation-modal',
  templateUrl: './flatten-confimation-modal.component.html',
  styleUrls: ['./flatten-confimation-modal.component.scss']
})
export class FlattenConfirmationModalComponent implements OnInit {

  child: string;
  parent: string;

  flattenForm: FormGroup;

  public onSubmit: Subject<string>;

  constructor(public modalRef: BsModalRef) {
    this.createForm();
  }

  createForm() {
    this.flattenForm = new FormGroup({});
  }

  ngOnInit() {
    this.onSubmit = new Subject();
  }

  submit() {
    this.onSubmit.next();
  }
}
