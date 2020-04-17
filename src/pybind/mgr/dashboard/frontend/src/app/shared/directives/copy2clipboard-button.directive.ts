import { Directive, ElementRef, HostListener, Input, OnInit, Renderer2 } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';

import { ToastrService } from 'ngx-toastr';

@Directive({
  selector: '[cdCopy2ClipboardButton]'
})
export class Copy2ClipboardButtonDirective implements OnInit {
  @Input()
  private cdCopy2ClipboardButton: string;
  @Input()
  private formatted = 'no';

  constructor(
    private elementRef: ElementRef,
    private renderer: Renderer2,
    private toastr: ToastrService,
    private i18n: I18n
  ) {}

  ngOnInit() {
    const iElement = this.renderer.createElement('i');
    this.renderer.addClass(iElement, 'fa');
    this.renderer.addClass(iElement, 'fa-clipboard');
    this.renderer.setAttribute(iElement, 'title', this.i18n('Copy to clipboard'));
    this.renderer.appendChild(this.elementRef.nativeElement, iElement);
  }

  private getInputElement() {
    return document.getElementById(this.cdCopy2ClipboardButton) as HTMLInputElement;
  }

  @HostListener('click')
  onClick() {
    try {
      const tagName = this.formatted === '' ? 'textarea' : 'input';
      // Create the input to hold our text.
      const tmpInputElement = document.createElement(tagName);
      tmpInputElement.value = this.getInputElement().value;
      document.body.appendChild(tmpInputElement);
      // Copy text to clipboard.
      tmpInputElement.select();
      document.execCommand('copy');
      // Finally remove the element.
      document.body.removeChild(tmpInputElement);

      this.toastr.success('Copied text to the clipboard successfully.');
    } catch (err) {
      this.toastr.error('Failed to copy text to the clipboard.');
    }
  }
}
