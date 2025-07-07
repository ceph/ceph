import { Component, Output, EventEmitter } from '@angular/core';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-notification-header',
  templateUrl: './notification-header.component.html',
  styleUrls: ['./notification-header.component.scss']
})
export class NotificationHeaderComponent {
  @Output() dismissAll = new EventEmitter<void>();
  
  isMuted = false;

  constructor(private notificationService: NotificationService) {}

  onDismissAll(): void {
    this.dismissAll.emit();
    this.notificationService.removeAll();
  }

  onToggleMute(): void {
    this.isMuted = !this.isMuted;
    this.notificationService.suspendToasties(this.isMuted);
  }
} 