import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ExecutingTask } from '~/app/shared/models/executing-task';

interface NotificationGroup {
  label: string;
  notifications: CdNotification[];
}

@Component({
  selector: 'cd-notification-area',
  template: `
    <!-- Executing tasks -->
    <cds-structured-list *ngIf="executingTasks.length > 0">
      <cds-list-row *ngFor="let task of executingTasks">
        <cds-list-column>
          <cds-tile class="info">
            <div class="notification-header">
              <svg cdsIcon="in-progress" size="16"></svg>
              <span class="notification-title">{{ task.description }}</span>
            </div>
            <div class="notification-footer">
              <div class="notification-meta">
                <span>In Progress</span>
              </div>
            </div>
          </cds-tile>
        </cds-list-column>
      </cds-list-row>
    </cds-structured-list>

    <!-- Notifications grouped by date -->
    <div *ngFor="let group of notificationGroups">
      <!-- Date separator -->
      <cds-tile class="date-separator">
        <div class="separator-content">
          <svg cdsIcon="calendar" size="16"></svg>
          <span class="separator-label">{{ group.label }}</span>
        </div>
      </cds-tile>

      <!-- Notifications for this group -->
      <cds-structured-list>
        <cds-list-row *ngFor="let notification of group.notifications; let i = index">
          <cds-list-column>
            <cds-tile [class]="getNotificationClass(notification)">
              <div class="notification-header">
                <svg [cdsIcon]="getNotificationIcon(notification)" size="16"></svg>
                <span class="notification-title">{{notification.title}}</span>
                <button cdsButton="ghost" size="sm" (click)="dismiss.emit(getNotificationIndex(notification))">
                  <svg cdsIcon="close" size="16"></svg>
                </button>
              </div>
              <div class="notification-message" [class.expanded]="expandedMessages[getNotificationIndex(notification)]" [innerHTML]="notification.message">
              </div>
              <div class="notification-footer">
                <div class="notification-meta">
                  <span class="notification-timestamp">{{notification.timestamp | date:'short'}}</span>
                  <span *ngIf="notification.application" class="notification-application">
                    {{notification.application}}
                  </span>             
                </div>
                <div class="notification-actions">
                  <button *ngIf="notification.message?.length > 100"
                          cdsButton="ghost" 
                          size="sm"
                          (click)="toggleMessage(getNotificationIndex(notification))">
                    {{expandedMessages[getNotificationIndex(notification)] ? 'Read less' : 'Read more'}}
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
          </cds-list-column>
        </cds-list-row>
      </cds-structured-list>
    </div>

    <!-- Empty state -->
    <div *ngIf="notifications.length === 0 && executingTasks.length === 0" class="text-center p-4">
      There are no notifications.
    </div>
  `,
  styleUrls: ['./notification-area.component.scss']
})
export class NotificationAreaComponent {
  @Input() notifications: CdNotification[] = [];
  @Input() executingTasks: ExecutingTask[] = [];
  @Output() dismiss = new EventEmitter<number>();
  @Output() retry = new EventEmitter<CdNotification>();
  @Output() toggleAlert = new EventEmitter<CdNotification>();
  @Output() clearAll = new EventEmitter<void>();

  expandedMessages: boolean[] = [];
  NotificationType = NotificationType;

  get notificationGroups(): NotificationGroup[] {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    
    const todayNotifications: CdNotification[] = [];
    const previousNotifications: CdNotification[] = [];

    this.notifications.forEach(notification => {
      const notificationDate = new Date(notification.timestamp);
      notificationDate.setHours(0, 0, 0, 0);
      
      if (notificationDate.getTime() === today.getTime()) {
        todayNotifications.push(notification);
      } else {
        previousNotifications.push(notification);
      }
    });

    const groups: NotificationGroup[] = [];
    
    if (todayNotifications.length > 0) {
      groups.push({
        label: 'Today',
        notifications: todayNotifications
      });
    }
    
    if (previousNotifications.length > 0) {
      groups.push({
        label: 'Previous',
        notifications: previousNotifications
      });
    }

    return groups;
  }

  getNotificationIndex(notification: CdNotification): number {
    return this.notifications.indexOf(notification);
  }

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