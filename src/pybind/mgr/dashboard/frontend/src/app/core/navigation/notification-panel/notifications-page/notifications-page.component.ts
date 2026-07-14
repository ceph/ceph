import {
  Component,
  OnInit,
  OnDestroy,
  ChangeDetectionStrategy,
  signal,
  computed
} from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { Location } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { Subscription } from 'rxjs';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdNotification } from '~/app/shared/models/cd-notification';
import { PrometheusAlertService } from '~/app/shared/services/prometheus-alert.service';
import { PrometheusNotificationService } from '~/app/shared/services/prometheus-notification.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { IconSize } from '~/app/shared/enum/icons.enum';

interface DisplayNotification extends CdNotification {
  displayTitle: string;
  displayPreview: string;
}

@Component({
  selector: 'cd-notifications-page',
  templateUrl: './notifications-page.component.html',
  styleUrls: ['./notifications-page.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class NotificationsPageComponent implements OnInit, OnDestroy {
  iconSize = IconSize;
  notifications = signal<DisplayNotification[]>([]);
  selectedNotificationID = signal<string | null>(null);
  readMap: ReturnType<typeof toSignal<Record<string, boolean>>>;

  selectedNotification = computed(() =>
    this.notifications().find((n) => n.id === this.selectedNotificationID())
  );

  hasNoNotifications = computed(() => this.notifications().length === 0);

  allRead = computed(() => {
    const map = this.readMap();
    return this.notifications().every((n) => map[n.id]);
  });

  private sub: Subscription;
  private interval: number;

  constructor(
    private notificationService: NotificationService,
    private prometheusAlertService: PrometheusAlertService,
    private prometheusNotificationService: PrometheusNotificationService,
    private authStorageService: AuthStorageService,
    private location: Location,
    private route: ActivatedRoute
  ) {
    this.readMap = toSignal(this.notificationService.readMap$, {
      initialValue: {} as Record<string, boolean>
    });
  }

  ngOnInit(): void {
    const permissions = this.authStorageService.getPermissions();
    if (permissions.prometheus.read && permissions.configOpt.read) {
      this.triggerPrometheusAlerts();
      this.interval = window.setInterval(() => {
        this.triggerPrometheusAlerts();
      }, 5000);
    }

    this.sub = this.notificationService.data$.subscribe((notifications) => {
      this.notifications.set(
        notifications.map((n) =>
          Object.assign(n, {
            displayTitle: n.prometheusAlert?.alertName || n.title || '',
            displayPreview: n.prometheusAlert?.description || n.message || ''
          })
        )
      );

      const id = this.route.snapshot.queryParams['id'];
      if (id && !this.selectedNotificationID()) {
        const match = notifications.find((n) => n.id === id);
        if (match) {
          this.selectedNotificationID.set(id);
          this.notificationService.markAsRead(id);
        }
      }
    });
  }

  ngOnDestroy(): void {
    this.sub?.unsubscribe();
    if (this.interval) {
      window.clearInterval(this.interval);
    }
  }

  goBack(): void {
    this.location.back();
  }

  markAllAsRead(): void {
    this.notificationService.markAllAsRead();
  }

  clearAll(): void {
    this.notificationService.removeAll();
    this.selectedNotificationID.set(null);
  }

  onNotificationSelect(notification: DisplayNotification): void {
    this.selectedNotificationID.set(notification.id);
    this.notificationService.markAsRead(notification.id);
  }

  removeNotification(notification: DisplayNotification, event: MouseEvent): void {
    event.stopPropagation();
    if (this.notificationService.removeById(notification.id)) {
      if (this.selectedNotificationID() === notification.id) {
        this.selectedNotificationID.set(null);
      }
    }
  }

  onNotificationDeleted(notificationId: string): void {
    if (this.selectedNotificationID() === notificationId) {
      this.selectedNotificationID.set(null);
    }
  }

  private triggerPrometheusAlerts(): void {
    this.prometheusAlertService.refresh();
    this.prometheusNotificationService.refresh();
  }
}
