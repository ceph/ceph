import { Component, HostListener, Input } from '@angular/core';

import { detect } from 'detect-browser';
import { ToastrService } from 'ngx-toastr';

import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-copy-2-clipboard-button',
  templateUrl: './copy2clipboard-button.component.html',
  styleUrls: ['./copy2clipboard-button.component.scss']
})
export class Copy2ClipboardButtonComponent {
  @Input()
  private source: string;

  @Input()
  byId = true;

  @Input()
  showIconOnly = false;

  icons = Icons;

  constructor(private toastr: ToastrService) {}

  private getText(): string {
    const element = document.getElementById(this.source) as HTMLInputElement;
    return element.value;
  }

  @HostListener('click')
  onClick() {
    try {
      const browser = detect();
      const text = this.byId ? this.getText() : this.source;
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
