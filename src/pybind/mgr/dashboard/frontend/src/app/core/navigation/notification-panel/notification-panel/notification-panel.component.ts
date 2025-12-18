import { Component, Input } from '@angular/core';
import { NotificationService } from '~/app/shared/services/notification.service';
import { trigger, transition, style, animate } from '@angular/animations';

@Component({
  selector: 'cd-notification-panel',
  templateUrl: './notification-panel.component.html',
  styleUrls: ['./notification-panel.component.scss'],
  standalone: false,
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
  @Input() isPanelOpen: boolean = true;

  constructor(public notificationService: NotificationService) {}
}
