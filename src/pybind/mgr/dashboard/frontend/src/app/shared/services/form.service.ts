import { ElementRef, Injectable } from '@angular/core';

@Injectable()
export class FormService {

  constructor() {
  }

  focusInvalid(el: ElementRef) {
    const target = el.nativeElement.querySelector('input.ng-invalid, select.ng-invalid');
    if (target) {
      target.focus();
    }
  }
}
