import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CdNotification } from '~/app/shared/models/cd-notification';

@Component({
  selector: 'cd-notification-area',
  template: `
    <div ibmGrid>
      <div ibmRow *ngFor="let notification of notifications; let i = index">
        <div ibmCol>
          <cds-tile>
            <div class="notification-header">
              <svg [cdsIcon]="getNotificationIcon(notification)" size="16"></svg>
              <span class="notification-title">{{notification.title}}</span>
              <button cdsButton="ghost" size="sm" (click)="dismiss.emit(i)">
                <svg cdsIcon="close" size="16"></svg>
              </button>
            </div>
            <p class="notification-message" [class.expanded]="expandedMessages[i]">
              {{notification.message}}
            </p>
            <div class="notification-footer">
              <span class="notification-timestamp">{{notification.timestamp | date:'short'}}</span>
              <button *ngIf="notification.message?.length > 100"
                      cdsButton="ghost" 
                      size="sm"
                      (click)="toggleMessage(i)">
                {{expandedMessages[i] ? 'Read less' : 'Read more'}}
              </button>
              <button *ngIf="notification.type === 2" 
                      cdsButton="ghost"
                      size="sm"
                      (click)="retry.emit(notification)">
                Retry
              </button>
            </div>
          </cds-tile>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .notification-header {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      margin-bottom: 0.5rem;
    }
    .notification-title {
      flex: 1;
      font-weight: 600;
    }
    .notification-message {
      margin: 0.5rem 0;
      max-height: 3rem;
      overflow: hidden;
    }
    .notification-message.expanded {
      max-height: none;
    }
    .notification-footer {
      display: flex;
      align-items: center;
      gap: 1rem;
    }
    .notification-timestamp {
      color: var(--cds-text-secondary);
      font-size: 0.875rem;
    }
  `]
})
export class NotificationAreaComponent {
  @Input() notifications: CdNotification[] = [];
  @Output() dismiss = new EventEmitter<number>();
  @Output() retry = new EventEmitter<CdNotification>();

  expandedMessages: boolean[] = [];

  getNotificationIcon(notification: CdNotification): string {
    switch (notification.type) {
      case 0: return 'error--filled';
      case 1: return 'success--filled';
      case 2: return 'warning--filled';
      default: return 'information--filled';
    }
  }

  toggleMessage(index: number) {
    this.expandedMessages[index] = !this.expandedMessages[index];
  }
} 