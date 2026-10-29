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

import { ElementRef } from '@angular/core';
import { NgForm, UntypedFormControl, Validators } from '@angular/forms';

import { CdFormControlDirective } from './cd-form-control.directive';

describe('CdFormControlDirective', () => {
  let directive: CdFormControlDirective;
  let mockControl: UntypedFormControl;
  let mockElementRef: ElementRef;

  beforeEach(() => {
    mockControl = new UntypedFormControl('', [Validators.required]);
    mockElementRef = {
      nativeElement: {
        validity: {
          badInput: false
        }
      }
    } as ElementRef;
    directive = new CdFormControlDirective(new NgForm([], []), mockElementRef);
    jest.spyOn(directive, 'control', 'get').mockReturnValue(mockControl);
  });

  it('should create an instance', () => {
    expect(directive).toBeTruthy();
  });

  it('should set pattern error and remove required error when badInput is true', () => {
    mockControl.setErrors({ required: true });
    (mockElementRef.nativeElement as any).validity.badInput = true;

    directive.onInput();

    expect(mockControl.hasError('pattern')).toBe(true);
    expect(mockControl.hasError('required')).toBe(false);
  });

  it('should preserve other errors like min and remove required when badInput is true', () => {
    mockControl.setErrors({ required: true, min: true });
    (mockElementRef.nativeElement as any).validity.badInput = true;

    directive.onInput();

    expect(mockControl.hasError('pattern')).toBe(true);
    expect(mockControl.hasError('min')).toBe(true);
    expect(mockControl.hasError('required')).toBe(false);
  });

  it('should not modify errors when badInput is false', () => {
    mockControl.setErrors({ required: true });
    (mockElementRef.nativeElement as any).validity.badInput = false;

    directive.onInput();

    expect(mockControl.hasError('required')).toBe(true);
    expect(mockControl.hasError('pattern')).toBe(false);
  });
});
