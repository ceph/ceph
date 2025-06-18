import { Component, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'cd-notification-header',
  template: `
    <div class="notification-header cds--g10">
      <div class="notification-content">
        <div class="notification-title">
          <cds-header>Tasks & Notifications</cds-header>
        </div>
        <div class="notification-controls">
          <div class="controls-group">
            <cds-toggle
              [hideLabel]="true"
              [label]="'Mute Notifications'"
              [checked]="doNotDisturb"
              (checkedChange)="doNotDisturbChange.emit($event)">
            </cds-toggle>
            <button 
              cdsButton="ghost"
              size="sm" 
              class="clear-btn"
              *ngIf="hasNotifications"
              (click)="clearAll.emit()">
              Dismiss all
            </button>
          </div>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .notification-header {
      padding: 1rem;
      background-color: var(--cds-layer-01);
      border-bottom: 1px solid var(--cds-border-subtle);
    }

    .notification-content {
      display: flex;
      flex-direction: column;
      gap: 2.5rem; /* gap between title and controls */
    }

    .notification-title {
      cds-header {
        margin: 0;
        font-size: 1rem;
        font-weight: 600;
        color: var(--cds-text-primary);
      }
    }

    .notification-controls {
      display: flex;
      align-items: center;
    }

    .controls-group {
      display: flex;
      align-items: center;
      gap: 1rem;

    }

    cds-toggle {
      margin: 0;
    }

    .clear-btn {
      min-height: 2rem;
      padding: 0 1rem;
    }
  `]
})
export class NotificationHeaderComponent {
  @Input() unreadCount = 0;
  @Input() doNotDisturb = false;
  @Input() hasNotifications = false;
  @Output() doNotDisturbChange = new EventEmitter<boolean>();
  @Output() clearAll = new EventEmitter<void>();
  @Output() close = new EventEmitter<void>();
} 