import { Directive, ElementRef, OnInit } from '@angular/core';

@Directive({
  selector: '[autofocus]' // tslint:disable-line
})
export class AutofocusDirective implements OnInit {

  constructor(private elementRef: ElementRef) {}

  ngOnInit() {
    setTimeout(() => {
      if (this.elementRef && this.elementRef.nativeElement) {
        this.elementRef.nativeElement.focus();
      }
    }, 0);
  }
}
