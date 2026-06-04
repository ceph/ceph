import { Component, Input, Output, EventEmitter } from '@angular/core';

import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-cephfs-download-token',
  templateUrl: './cephfs-download-token.component.html',
  styleUrls: ['./cephfs-download-token.component.scss'],
  standalone: false
})
export class CephfsDownloadTokenComponent {
  @Input() open = false;
  @Input() token = '';
  @Input() siteName = '';
  @Input() filesystemName = '';
  @Output() closed = new EventEmitter<void>();

  constructor(private notificationService: NotificationService) {}

  copyToken(): void {
    const el = document.getElementById('secureToken') as HTMLTextAreaElement;
    const text = el?.value || el?.textContent || '';
    navigator.clipboard.writeText(text).then(
      () =>
        this.notificationService.show(
          NotificationType.success,
          $localize`Success`,
          $localize`Copied text to the clipboard successfully.`
        ),
      () =>
        this.notificationService.show(
          NotificationType.error,
          $localize`Error`,
          $localize`Failed to copy text to the clipboard.`
        )
    );
  }

  downloadToken(): void {
    if (!this.token) return;
    const blob = new Blob([this.token], { type: 'text/plain' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cephfs-bootstrap-token-${this.siteName || this.filesystemName}.txt`;
    a.click();
    window.URL.revokeObjectURL(url);
  }

  onClose(): void {
    this.closed.emit();
  }
}
