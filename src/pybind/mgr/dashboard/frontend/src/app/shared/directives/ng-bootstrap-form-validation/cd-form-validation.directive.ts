/**
 * MIT License
 *
 * Copyright (c) 2017 Kevin Kipp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 *
 * Based on https://github.com/third774/ng-bootstrap-form-validation
 */

import { Directive, EventEmitter, HostListener, Input, Output } from '@angular/core';
import {
  AbstractControl,
  UntypedFormArray,
  UntypedFormControl,
  UntypedFormGroup
} from '@angular/forms';

@Directive({
  // eslint-disable-next-line
  selector: '[formGroup]'
})
export class CdFormValidationDirective {
  @Input()
  formGroup: UntypedFormGroup;
  @Output()
  validSubmit = new EventEmitter<any>();

  @HostListener('submit')
  onSubmit() {
    this.markAsTouchedAndDirty(this.formGroup);
    if (this.formGroup.valid) {
      this.validSubmit.emit(this.formGroup.value);
    }
  }

  markAsTouchedAndDirty(control: AbstractControl) {
    if (control instanceof UntypedFormGroup) {
      Object.keys(control.controls).forEach((key) =>
        this.markAsTouchedAndDirty(control.controls[key])
      );
    } else if (control instanceof UntypedFormArray) {
      control.controls.forEach((c) => this.markAsTouchedAndDirty(c));
    } else if (control instanceof UntypedFormControl && control.enabled) {
      control.markAsDirty();
      control.markAsTouched();
      control.updateValueAndValidity();
    }
  }
}
