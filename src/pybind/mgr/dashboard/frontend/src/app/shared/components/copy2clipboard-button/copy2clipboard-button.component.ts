import { Component, EventEmitter, HostListener, Input, Output } from '@angular/core';

import { detect } from 'detect-browser';

import { Icons } from '~/app/shared/enum/icons.enum';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';

const ERROR_TITLE = $localize`Error`;
const CLIPBOARD_ERROR_MESSAGE = $localize`Failed to copy text to the clipboard.`;
const SUCCESS_TITLE = $localize`Success`;
const CLIPBOARD_SUCCESS_MESSAGE = $localize`Copied text to the clipboard successfully.`;

@Component({
  selector: 'cd-copy-2-clipboard-button',
  templateUrl: './copy2clipboard-button.component.html',
  styleUrls: ['./copy2clipboard-button.component.scss']
})
export class Copy2ClipboardButtonComponent {
  // The text to be copied
  @Input()
  private source: string;

  // Optional: Extracts text to be copied by "id" attribute
  @Input()
  byId = true;

  // Size of the button
  @Input()
  size = 'md';

  // Optional: Adds text to the left of copy icon
  @Input()
  text?: string;

  @Output()
  toastSuccess = new EventEmitter<void>();

  @Output()
  toastError = new EventEmitter<void>();

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
          SUCCESS_TITLE,
          CLIPBOARD_SUCCESS_MESSAGE
        );
        this.toastSuccess.emit();
      };
      const showError = () => {
        this.notificationService.show(NotificationType.error, ERROR_TITLE, CLIPBOARD_ERROR_MESSAGE);
        this.toastError.emit();
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
      this.notificationService.show(NotificationType.error, ERROR_TITLE, CLIPBOARD_ERROR_MESSAGE);
      this.toastError.emit();
    }
  }
}
