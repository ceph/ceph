import { Component, Input, Output, EventEmitter, inject } from '@angular/core';

import { NotificationService } from '~/app/shared/services/notification.service';
import { TextToDownloadService } from '~/app/shared/services/text-to-download.service';
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

  private notificationService = inject(NotificationService);
  private textToDownloadService = inject(TextToDownloadService);

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
    const label = (this.siteName || this.filesystemName || 'token').replace(/[^a-zA-Z0-9_-]/g, '_');
    this.textToDownloadService.download(this.token, `cephfs-bootstrap-token-${label}.txt`);
  }

  onClose(): void {
    this.closed.emit();
  }
}
