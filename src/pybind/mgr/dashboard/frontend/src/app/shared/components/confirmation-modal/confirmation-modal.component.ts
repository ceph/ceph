import { Component, OnInit, TemplateRef } from '@angular/core';
import { FormGroup } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap';

@Component({
  selector: 'cd-confirmation-modal',
  templateUrl: './confirmation-modal.component.html',
  styleUrls: ['./confirmation-modal.component.scss']
})
export class ConfirmationModalComponent implements OnInit {
  bodyData: object;
  bodyTpl: TemplateRef<any>;
  buttonText: string;
  onSubmit: Function;
  titleText: string;

  bodyContext: object;
  confirmationForm: FormGroup;

  constructor(public modalRef: BsModalRef) {
    this.confirmationForm = new FormGroup({});
  }

  ngOnInit() {
    this.bodyContext = {
      $implicit: this.bodyData
    };
  }

  submit() {
    this.onSubmit();
  }

  stopLoadingSpinner() {
    this.confirmationForm.setErrors({ cdSubmitButton: true });
  }
}
