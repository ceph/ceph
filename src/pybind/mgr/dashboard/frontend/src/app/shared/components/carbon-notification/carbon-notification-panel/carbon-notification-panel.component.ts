import { Component, Input, Output, EventEmitter, HostBinding } from '@angular/core';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { ExecutingTask } from '~/app/shared/models/executing-task';

@Component({
  selector: 'cd-carbon-notification-panel',
  template: `
    <div class="notification-panel cds--g10">
      <cd-notification-header
        [unreadCount]="unreadCount"
        [doNotDisturb]="doNotDisturb"
        [hasNotifications]="notifications.length > 0"
        (doNotDisturbChange)="doNotDisturbChange.emit($event)"
        (clearAll)="clearAllNotifications.emit()"
        (close)="showCarbonPanel = false">
      </cd-notification-header>

      <cd-notification-area
        [notifications]="notifications"
        [executingTasks]="executingTasks"
        (dismiss)="dismissNotification.emit($event)"
        (retry)="retryNotification.emit($event)"
        (toggleAlert)="toggleAlert.emit($event)"
        (clearAll)="clearAllNotifications.emit()">
      </cd-notification-area>

      <cd-notification-footer
        (viewAll)="viewAllNotifications.emit()"
        (settings)="openSettings.emit()">
      </cd-notification-footer>
    </div>
  `,
  styles: [`
    .notification-panel {
      height: 100%;
      display: flex;
      flex-direction: column;
      background-color: var(--cds-layer-01);
      color: var(--cds-text-primary);
    }

    cd-notification-area {
      flex: 1;
      overflow-y: auto;
      padding: 1rem;
      background-color: var(--cds-layer-01);
    }

    cd-notification-footer {
      padding: 1rem;
      border-top: 1px solid var(--cds-border-subtle);
      background-color: var(--cds-layer-01);
    }
  `]
})
export class CarbonNotificationPanelComponent {
  @Input() notifications: CdNotification[] = [];
  @Input() executingTasks: ExecutingTask[] = [];
  @Input() unreadCount = 0;
  @Input() doNotDisturb = false;
  @Input() showCarbonPanel = false;

  @Output() doNotDisturbChange = new EventEmitter<boolean>();
  @Output() dismissNotification = new EventEmitter<number>();
  @Output() retryNotification = new EventEmitter<CdNotification>();
  @Output() toggleAlert = new EventEmitter<CdNotification>();
  @Output() viewAllNotifications = new EventEmitter<void>();
  @Output() openSettings = new EventEmitter<void>();
  @Output() clearAllNotifications = new EventEmitter<void>();

  @HostBinding('class.open') get isOpen() {
    return this.showCarbonPanel;
  }

  @HostBinding('class.cds--g10') g10Theme = true;
} 