import { Directive, HostListener } from '@angular/core';
import { NgControl } from '@angular/forms';

import * as _ from 'lodash';

@Directive({
  selector: '[cdTrim]'
})
export class TrimDirective {
  constructor(private ngControl: NgControl) {}

  @HostListener('input', ['$event.target.value'])
  onInput(value) {
    this.setValue(value);
  }

  setValue(value: string): void {
    value = _.isString(value) ? value.trim() : value;
    this.ngControl.control.setValue(value);
  }
}
