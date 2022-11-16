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

import {
  ContentChildren,
  Directive,
  ElementRef,
  HostBinding,
  Input,
  QueryList
} from '@angular/core';
import { FormControlName } from '@angular/forms';

@Directive({
  // eslint-disable-next-line
  selector: '.form-group'
})
export class CdFormGroupDirective {
  @ContentChildren(FormControlName)
  formControlNames: QueryList<FormControlName>;

  @Input()
  validationDisabled = false;

  @HostBinding('class.has-error')
  get hasErrors() {
    return (
      this.formControlNames.some((c) => !c.valid && c.dirty && c.touched) &&
      !this.validationDisabled
    );
  }

  @HostBinding('class.has-success')
  get hasSuccess() {
    return (
      !this.formControlNames.some((c) => !c.valid) &&
      this.formControlNames.some((c) => c.dirty && c.touched) &&
      !this.validationDisabled
    );
  }

  constructor(private elRef: ElementRef) {}

  get label() {
    const label = this.elRef.nativeElement.querySelector('label');
    return label && label.textContent ? label.textContent.trim() : 'This field';
  }

  get isDirtyAndTouched() {
    return this.formControlNames.some((c) => c.dirty && c.touched);
  }
}
