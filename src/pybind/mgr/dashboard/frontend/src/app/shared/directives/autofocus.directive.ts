import { AfterViewInit, Directive, ElementRef, Input } from '@angular/core';

import _ from 'lodash';

@Directive({
  selector: '[autofocus]' // eslint-disable-line
})
export class AutofocusDirective implements AfterViewInit {
  private focus = true;

  constructor(private elementRef: ElementRef) {}

  ngAfterViewInit() {
    const el: HTMLInputElement = this.elementRef.nativeElement;
    if (this.focus && _.isFunction(el.focus)) {
      el.focus();
    }
  }

  @Input()
  public set autofocus(condition: any) {
    if (_.isBoolean(condition)) {
      this.focus = condition;
    } else if (_.isFunction(condition)) {
      this.focus = condition();
    }
  }
}
