import { Directive } from '@angular/core';
import { NG_VALUE_ACCESSOR, ControlValueAccessor } from '@angular/forms';
@Directive({
  // eslint-disable-next-line
  selector: 'input[type=file]',
  // eslint-disable-next-line
  host: {
    '(change)': 'onChange($event.target.files)',
    '(input)': 'onChange($event.target.files)',
    '(blur)': 'onTouched()'
  },
  providers: [
    { provide: NG_VALUE_ACCESSOR, useExisting: FormlyFileValueAccessorDirective, multi: true }
  ]
})
// https://github.com/angular/angular/issues/7341
export class FormlyFileValueAccessorDirective implements ControlValueAccessor {
  value: any;
  onChange = (_: any) => {};
  onTouched = () => {};

  writeValue(_value: any) {}
  registerOnChange(fn: any) {
    this.onChange = fn;
  }
  registerOnTouched(fn: any) {
    this.onTouched = fn;
  }
}
