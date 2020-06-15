import { Directive, ElementRef, HostListener, Input, OnInit, Renderer2 } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { ToastrService } from 'ngx-toastr';

@Directive({
  selector: '[cdCopy2ClipboardButton]'
})
export class Copy2ClipboardButtonDirective implements OnInit {
  @Input()
  private cdCopy2ClipboardButton: string;

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
      // Checking if we have the clipboard-write permission
      navigator.permissions
        .query({ name: 'clipboard-write' as PermissionName })
        .then((result: any) => {
          if (result.state === 'granted' || result.state === 'prompt') {
            // Copy text to clipboard.
            navigator.clipboard.writeText(this.getInputElement().value);
          }
        });
      this.toastr.success('Copied text to the clipboard successfully.');
    } catch (err) {
      this.toastr.error('Failed to copy text to the clipboard.');
    }
  }
}
