import { AfterViewInit, Directive, ElementRef, Input, Renderer2 } from '@angular/core';

@Directive({
  selector: '[cdRequiredField]'
})
export class RequiredFieldDirective implements AfterViewInit {
  @Input('cdRequiredField') label: string;
  @Input() skeleton: boolean;
  constructor(private elementRef: ElementRef, private renderer: Renderer2) {}

  ngAfterViewInit() {
    if (!this.label || this.skeleton) return;
    const labelElement = this.elementRef.nativeElement.querySelector('.cds--label');

    if (labelElement) {
      this.renderer.setProperty(labelElement, 'textContent', `${this.label} (required)`);
    }
  }
}
