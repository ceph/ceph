import { trigger, transition, style, animate } from '@angular/animations';
import { Component, ElementRef, HostListener, inject } from '@angular/core';

import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  standalone: false,
  selector: 'cd-notification-panel',
  templateUrl: './notification-panel.component.html',
  styleUrls: ['./notification-panel.component.scss'],
  animations: [
    trigger('panelAnimation', [
      transition(':enter', [
        style({ opacity: 0, transform: 'translateY(-38.5rem)' }),
        animate(
          '240ms cubic-bezier(0.2, 0, 0.38, 0.9)',
          style({ opacity: 1, transform: 'translateY(0)' })
        )
      ])
    ])
  ]
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
