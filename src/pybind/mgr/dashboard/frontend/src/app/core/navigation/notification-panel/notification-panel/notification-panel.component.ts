import { Component, ElementRef, HostListener, inject } from '@angular/core';

import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-notification-panel',
  templateUrl: './notification-panel.component.html',
  styleUrls: ['./notification-panel.component.scss'],
  standalone: false
})
export class NotificationPanelComponent {
  private elementRef = inject(ElementRef);
  private notificationService = inject(NotificationService);

  @HostListener('document:click', ['$event.target'])
  onClickOutside(target: HTMLElement): void {
    if (
      !this.elementRef.nativeElement.contains(target) &&
      !target.closest('[data-testid="header-notification-icon"]')
    ) {
      this.notificationService.setPanelState(false);
    }
  }

  @HostListener('document:keydown.escape')
  onEscape(): void {
    this.notificationService.setPanelState(false);
  }
}
