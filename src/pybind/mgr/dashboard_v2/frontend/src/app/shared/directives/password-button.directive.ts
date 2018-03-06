import { Directive, ElementRef, HostListener, Input, OnInit, Renderer2 } from '@angular/core';

@Directive({
  selector: '[cdPasswordButton]'
})
export class PasswordButtonDirective implements OnInit {
  private inputElement: any;
  private iElement: any;

  @Input('cdPasswordButton') private cdPasswordButton: string;

  constructor(private el: ElementRef, private renderer: Renderer2) { }

  ngOnInit() {
    this.inputElement = document.getElementById(this.cdPasswordButton);
    this.iElement = this.renderer.createElement('i');
    this.renderer.addClass(this.iElement, 'icon-prepend');
    this.renderer.addClass(this.iElement, 'fa');
    this.renderer.appendChild(this.el.nativeElement, this.iElement);
    this.update();
  }

  private update() {
    if (this.inputElement.type === 'text') {
      this.renderer.removeClass(this.iElement, 'fa-eye');
      this.renderer.addClass(this.iElement, 'fa-eye-slash');
    } else {
      this.renderer.removeClass(this.iElement, 'fa-eye-slash');
      this.renderer.addClass(this.iElement, 'fa-eye');
    }
  }

  @HostListener('click')
  onClick() {
    // Modify the type of the input field.
    this.inputElement.type = (this.inputElement.type === 'password') ? 'text' : 'password';
    // Update the button icon/tooltip.
    this.update();
  }
}
