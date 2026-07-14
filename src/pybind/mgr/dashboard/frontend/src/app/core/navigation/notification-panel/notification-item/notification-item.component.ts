import { Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import {
  NotificationApplication,
  NotificationType
} from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-notification-item',
  templateUrl: './notification-item.component.html',
  styleUrls: ['./notification-item.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class NotificationItemComponent {
  /* Notification severity */
  @Input() type: NotificationType = NotificationType.info;
  /* Notification heading text */
  @Input() title: string = '';
  /* Timestamp value */
  @Input() timestamp: Date | string | number;
  /* Notification body text or preview */
  @Input() message: string = '';
  /* Unique notification id — required for delete */
  @Input() notificationId: string;
  /* Source application — renders logo and absolute date */
  @Input() application: string;
  /* Show chevron instead of delete button */
  @Input() showChevron: boolean = false;
  /* Replace severity icon with unread dot */
  @Input() showUnread: boolean = false;
  /* Emitted after notification is deleted via the service */
  @Output() deleted = new EventEmitter<string>();

  readonly NotificationApplication = NotificationApplication;

  constructor(private notificationService: NotificationService) {}

  readonly iconMap = {
    [NotificationType.success]: 'success',
    [NotificationType.error]: 'error',
    [NotificationType.info]: 'infoCircle',
    [NotificationType.warning]: 'warning',
    default: 'infoCircle'
  } as const;

  onDelete(event: Event): void {
    event.stopPropagation();
    if (this.notificationService.removeById(this.notificationId)) {
      this.deleted.emit(this.notificationId);
    }
  }
}
