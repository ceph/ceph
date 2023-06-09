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

import { Directive, Host, HostBinding, Input, Optional, SkipSelf } from '@angular/core';
import { ControlContainer, UntypedFormControl } from '@angular/forms';

export function controlPath(name: string, parent: ControlContainer): string[] {
  // tslint:disable-next-line:no-non-null-assertion
  return [...parent.path!, name];
}

@Directive({
  // eslint-disable-next-line
  selector: '.form-control,.form-check-input,.custom-control-input'
})
export class CdFormControlDirective {
  @Input()
  formControlName: string;
  @Input()
  formControl: string;

  @HostBinding('class.is-valid')
  get validClass() {
    if (!this.control) {
      return false;
    }
    return this.control.valid && (this.control.touched || this.control.dirty);
  }

  @HostBinding('class.is-invalid')
  get invalidClass() {
    if (!this.control) {
      return false;
    }
    return this.control.invalid && this.control.touched && this.control.dirty;
  }

  get path() {
    return controlPath(this.formControlName, this.parent);
  }

  get control(): UntypedFormControl {
    return this.formDirective && this.formDirective.getControl(this);
  }

  get formDirective(): any {
    return this.parent ? this.parent.formDirective : null;
  }

  constructor(
    // this value might be null, but we union type it as such until
    // this issue is resolved: https://github.com/angular/angular/issues/25544
    @Optional()
    @Host()
    @SkipSelf()
    private parent: ControlContainer
  ) {}
}
