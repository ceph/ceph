import { AfterViewInit, Directive, ElementRef, Input, Renderer2 } from '@angular/core';

@Directive({
  selector: '[cdRequiredField]'
})
export class RequiredFieldDirective implements AfterViewInit {
  @Input('cdRequiredField') label: string;
  constructor(private elementRef: ElementRef, private renderer: Renderer2) {}

  ngAfterViewInit() {
    const labelElement = this.elementRef.nativeElement.querySelector('.cds--label');

    if (labelElement) {
      this.renderer.setProperty(labelElement, 'textContent', `${this.label} (required)`);
    }
  }
}
