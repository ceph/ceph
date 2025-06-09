import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-notification-area',
  template: `
    <cds-grid>
      <cds-row *ngFor="let notification of notifications; let i = index">
        <cds-col>
          <cds-tile [class]="getNotificationClass(notification)">
            <div class="notification-header">
              <svg [cdsIcon]="getNotificationIcon(notification)" size="16"></svg>
              <span class="notification-title">{{notification.title}}</span>
              <button cdsButton="ghost" size="sm" (click)="dismiss.emit(i)">
                <svg cdsIcon="close" size="16"></svg>
              </button>
            </div>
            <div class="notification-message" [class.expanded]="expandedMessages[i]" [innerHTML]="notification.message">
            </div>
            <div class="notification-footer">
              <div class="notification-meta">
                <span class="notification-timestamp">{{notification.timestamp | date:'short'}}</span>
                <span *ngIf="notification.application" class="notification-application">
                  {{notification.application}}
                </span>
                <span *ngIf="notification.duration" class="notification-duration">
                  Duration: {{notification.duration}}
                </span>
              </div>
              <div class="notification-actions">
                <button *ngIf="notification.message?.length > 100"
                        cdsButton="ghost" 
                        size="sm"
                        (click)="toggleMessage(i)">
                  {{expandedMessages[i] ? 'Read less' : 'Read more'}}
                </button>
                <button *ngIf="notification.type === NotificationType.error" 
                        cdsButton="ghost"
                        size="sm"
                        (click)="retry.emit(notification)">
                  Retry
                </button>
                <button *ngIf="notification.application === 'Prometheus' && notification.type !== NotificationType.success"
                        cdsButton="ghost"
                        size="sm"
                        (click)="toggleAlert.emit(notification)">
                  {{notification.alertSilenced ? 'Unsilence' : 'Silence'}}
                </button>
              </div>
            </div>
          </cds-tile>
        </cds-col>
      </cds-row>
    </cds-grid>
  `,
  styleUrls: ['./notification-area.component.scss']
})
export class NotificationAreaComponent {
  @Input() notifications: CdNotification[] = [];
  @Output() dismiss = new EventEmitter<number>();
  @Output() retry = new EventEmitter<CdNotification>();
  @Output() toggleAlert = new EventEmitter<CdNotification>();

  expandedMessages: boolean[] = [];
  NotificationType = NotificationType;

  getNotificationIcon(notification: CdNotification): string {
    switch (notification.type) {
      case NotificationType.error: return 'error--filled';
      case NotificationType.success: return 'checkmark--filled';
      case NotificationType.info: return 'information--filled';
      default: return 'warning--filled';
    }
  }

  getNotificationClass(notification: CdNotification): string {
    switch (notification.type) {
      case NotificationType.error: return 'error';
      case NotificationType.success: return 'success';
      case NotificationType.info: return 'info';
      default: return 'warning';
    }
  }

  toggleMessage(index: number) {
    this.expandedMessages[index] = !this.expandedMessages[index];
  }
} 