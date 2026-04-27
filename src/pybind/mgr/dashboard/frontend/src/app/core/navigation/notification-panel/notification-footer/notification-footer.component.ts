import { Component } from '@angular/core';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-notification-footer',
  templateUrl: './notification-footer.component.html',
  styleUrls: ['./notification-footer.component.scss'],
  standalone: false
})
export class NotificationFooterComponent {
  constructor(public notificationService: NotificationService) {}

  closePanel(event: Event) {
    event.preventDefault();
    event.stopPropagation();
    this.notificationService.setPanelState(false);
  }
}
