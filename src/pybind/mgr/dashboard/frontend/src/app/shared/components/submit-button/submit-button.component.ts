import { Component, ElementRef, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { AbstractControl, FormGroup, FormGroupDirective, NgForm } from '@angular/forms';

import * as _ from 'lodash';

import { Icons } from '../../../shared/enum/icons.enum';

/**
 * This component will render a submit button with the given label.
 *
 * The button will disabled itself and show a loading icon when the user clicks
 * it, usually initiating a request to the server, and it will stay in that
 * state until the request is finished.
 *
 * To indicate that the request failed, returning the button to the enable
 * state, you need to insert an error in the form with the 'cdSubmitButton' key.
 * p.e.: this.rbdForm.setErrors({'cdSubmitButton': true});
 *
 * It will also check if the form is valid, when clicking the button, and will
 * focus on the first invalid input.
 *
 * @export
 * @class SubmitButtonComponent
 * @implements {OnInit}
 */
@Component({
  selector: 'cd-submit-button',
  templateUrl: './submit-button.component.html',
  styleUrls: ['./submit-button.component.scss']
})
export class SubmitButtonComponent implements OnInit {
  @Input()
  form: FormGroup | NgForm;

  @Input()
  type = 'submit';

  @Input()
  disabled = false;

  // A CSS class string to apply to the button's main element.
  @Input()
  btnClass: string;

  @Output()
  submitAction = new EventEmitter();

  loading = false;
  icons = Icons;

  constructor(private elRef: ElementRef) {}

  ngOnInit() {
    this.form.statusChanges.subscribe(() => {
      if (_.has(this.form.errors, 'cdSubmitButton')) {
        this.loading = false;
        _.unset(this.form.errors, 'cdSubmitButton');
        // Handle Reactive forms.
        if (this.form instanceof AbstractControl) {
          (<AbstractControl>this.form).updateValueAndValidity();
        }
      }
    });
  }

  submit($event: any) {
    this.focusButton();

    // Special handling for Template driven forms.
    if (this.form instanceof FormGroupDirective) {
      (<FormGroupDirective>this.form).onSubmit($event);
    }

    if (this.form.invalid) {
      this.focusInvalid();
      return;
    }

    this.loading = true;
    this.submitAction.emit();
  }

  focusButton() {
    this.elRef.nativeElement.offsetParent.querySelector(`button[type="${this.type}"]`).focus();
  }

  focusInvalid() {
    const target = this.elRef.nativeElement.offsetParent.querySelector(
      'input.ng-invalid, select.ng-invalid'
    );

    if (target) {
      target.focus();
    }
  }
}
