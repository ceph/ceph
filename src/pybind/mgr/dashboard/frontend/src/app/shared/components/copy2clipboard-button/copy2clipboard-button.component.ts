import { Component, HostListener, Input } from '@angular/core';

import { detect } from 'detect-browser';

import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';

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

  constructor(private notificationService: NotificationService) {}

  private getText(): string {
    const element = document.getElementById(this.source) as HTMLInputElement;
    return element?.value || element?.textContent;
  }

  @HostListener('click')
  onClick() {
    try {
      const browser = detect();
      const text = this.byId ? this.getText() : this.source;
      const showSuccess = () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Success`,
          $localize`Copied text to the clipboard successfully.`
        );
      };
      const showError = () => {
        this.notificationService.show(
          NotificationType.error,
          $localize`Error`,
          $localize`Failed to copy text to the clipboard.`
        );
      };
      if (['firefox', 'ie', 'ios', 'safari'].includes(browser.name)) {
        // Various browsers do not support the `Permissions API`.
        // https://developer.mozilla.org/en-US/docs/Web/API/Permissions_API#Browser_compatibility
        navigator.clipboard
          .writeText(text)
          .then(() => showSuccess())
          .catch(() => showError());
      } else {
        // Checking if we have the clipboard-write permission
        navigator.permissions
          .query({ name: 'clipboard-write' as PermissionName })
          .then((result: any) => {
            if (result.state === 'granted' || result.state === 'prompt') {
              navigator.clipboard
                .writeText(text)
                .then(() => showSuccess())
                .catch(() => showError());
            }
          })
          .catch(() => showError());
      }
    } catch (_) {
      this.notificationService.show(
        NotificationType.error,
        $localize`Error`,
        $localize`Failed to copy text to the clipboard.`
      );
    }
  }
}
