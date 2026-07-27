import { Component, ChangeDetectionStrategy, inject, Signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { map } from 'rxjs/operators';

import { ICON_TYPE, IconSize } from '~/app/shared/enum/icons.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-notifications',
  templateUrl: './notifications.component.html',
  styleUrls: ['./notifications.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class NotificationsComponent {
  icons = ICON_TYPE;
  iconSize = IconSize.size20;

  private notificationService = inject(NotificationService);
  private summaryService = inject(SummaryService);

  hasRunningTasks: Signal<boolean> = toSignal(
    this.summaryService.summaryData$.pipe(map((summary) => summary?.executing_tasks?.length > 0)),
    { initialValue: false }
  );

  isMuted = toSignal(this.notificationService.muteState$, { initialValue: false });
  hasNotifications = toSignal(this.notificationService.hasUnread$, { initialValue: false });
}
