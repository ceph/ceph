import { Component, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs/Subject';

@Component({
  selector: 'cd-delete-confirmation-modal',
  templateUrl: './delete-confirmation-modal.component.html',
  styleUrls: ['./delete-confirmation-modal.component.scss']
})
export class DeleteConfirmationComponent implements OnInit {

  itemName: string;

  deleteForm: FormGroup;

  public onSubmit: Subject<string>;

  constructor(public modalRef: BsModalRef) {
    this.createForm();
  }

  createForm() {
    this.deleteForm = new FormGroup({});
  }

  ngOnInit() {
    this.onSubmit = new Subject();
  }

  submit() {
    this.onSubmit.next(this.itemName);
  }
}
