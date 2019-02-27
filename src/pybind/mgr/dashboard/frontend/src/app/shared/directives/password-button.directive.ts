import { Directive, ElementRef, HostListener, Input, OnInit, Renderer2 } from '@angular/core';

@Directive({
  selector: '[cdPasswordButton]'
})
export class PasswordButtonDirective implements OnInit {
  private iElement: HTMLElement;

  @Input() private cdPasswordButton: string;

  constructor(private elementRef: ElementRef,
              private renderer: Renderer2) {}

  ngOnInit() {
    this.iElement = this.renderer.createElement('i');
    this.renderer.addClass(this.iElement, 'icon-prepend');
    this.renderer.addClass(this.iElement, 'fa');
    this.renderer.appendChild(this.elementRef.nativeElement, this.iElement);
    this.update();
  }

  private getInputElement() {
    return document.getElementById(this.cdPasswordButton) as HTMLInputElement;
  }

  private update() {
    const inputElement = this.getInputElement();
    if (inputElement && (inputElement.type === 'text')) {
      this.renderer.removeClass(this.iElement, 'fa-eye');
      this.renderer.addClass(this.iElement, 'fa-eye-slash');
    } else {
      this.renderer.removeClass(this.iElement, 'fa-eye-slash');
      this.renderer.addClass(this.iElement, 'fa-eye');
    }
  }

  @HostListener('click')
  onClick() {
    const inputElement = this.getInputElement();
    // Modify the type of the input field.
    inputElement.type = (inputElement.type === 'password') ? 'text' : 'password';
    // Update the button icon/tooltip.
    this.update();
  }
}
