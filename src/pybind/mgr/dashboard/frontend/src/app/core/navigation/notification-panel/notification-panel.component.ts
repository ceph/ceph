import { Component, ElementRef, HostListener } from '@angular/core';
import { NotificationService } from '../../../shared/services/notification.service';

@Component({
  selector: 'cd-notification-panel',
  templateUrl: './notification-panel.component.html',
  styleUrls: ['./notification-panel.component.scss']
})
export class NotificationPanelComponent {
  constructor(public notificationService: NotificationService, private elementRef: ElementRef) {}

  @HostListener('document:click', ['$event'])
  handleClickOutside(event: Event) {
    const clickedInside = this.elementRef.nativeElement.contains(event.target);
    if (!clickedInside) {
      this.notificationService.toggleSidebar(false, true);
    }
  }
}
