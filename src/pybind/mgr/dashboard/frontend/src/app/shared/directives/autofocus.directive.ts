import { AfterViewInit, Directive, ElementRef } from '@angular/core';

import * as _ from 'lodash';

@Directive({
  selector: '[autofocus]' // tslint:disable-line
})
export class AutofocusDirective implements AfterViewInit {
  constructor(private elementRef: ElementRef) {}

  ngAfterViewInit() {
    const el: HTMLInputElement = this.elementRef.nativeElement;
    if (_.isFunction(el.focus)) {
      el.focus();
    }
  }
}
