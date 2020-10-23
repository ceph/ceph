import { Directive, ElementRef, HostListener, Input, OnInit, Renderer2 } from '@angular/core';

import { detect } from 'detect-browser';
import { ToastrService } from 'ngx-toastr';

@Directive({
  selector: '[cdCopy2ClipboardButton]'
})
export class Copy2ClipboardButtonDirective implements OnInit {
  @Input()
  private cdCopy2ClipboardButton: string;
  @Input()
  byId = true;

  constructor(
    private elementRef: ElementRef,
    private renderer: Renderer2,
    private toastr: ToastrService
  ) {}

  ngOnInit() {
    const iElement = this.renderer.createElement('i');
    this.renderer.addClass(iElement, 'fa');
    this.renderer.addClass(iElement, 'fa-clipboard');
    this.renderer.setAttribute(iElement, 'title', $localize`Copy to clipboard`);
    this.renderer.appendChild(this.elementRef.nativeElement, iElement);
  }

  private getText(): string {
    const element = document.getElementById(this.cdCopy2ClipboardButton) as HTMLInputElement;
    return element.value;
  }

  @HostListener('click')
  onClick() {
    try {
      const browser = detect();
      const text = this.byId ? this.getText() : this.cdCopy2ClipboardButton;
      const toastrFn = () => {
        this.toastr.success('Copied text to the clipboard successfully.');
      };
      if (['firefox', 'ie', 'ios', 'safari'].includes(browser.name)) {
        // Various browsers do not support the `Permissions API`.
        // https://developer.mozilla.org/en-US/docs/Web/API/Permissions_API#Browser_compatibility
        navigator.clipboard.writeText(text).then(() => toastrFn());
      } else {
        // Checking if we have the clipboard-write permission
        navigator.permissions
          .query({ name: 'clipboard-write' as PermissionName })
          .then((result: any) => {
            if (result.state === 'granted' || result.state === 'prompt') {
              navigator.clipboard.writeText(text).then(() => toastrFn());
            }
          });
      }
    } catch (_) {
      this.toastr.error('Failed to copy text to the clipboard.');
    }
  }
}
