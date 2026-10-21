import { Injectable } from '@angular/core';

import { Subscription } from 'rxjs';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { ExecutingTask } from '../models/executing-task';
import { NotificationService } from './notification.service';
import { SummaryService } from './summary.service';

const MIGRATION_TASK_PREFIX = "progress/Migrating pool '";

/**
 * The migrate endpoint returns as soon as the mon command is accepted, long before the data
 * has moved. The real progress is reported by the backend as an executing task, so the
 * "finished" notification is emitted when that task is no longer reported.
 */
@Injectable({
  providedIn: 'root'
})
export class PoolMigrationNotificationService {
  private runningMigrations = new Map<string, ExecutingTask>();

  constructor(
    private summaryService: SummaryService,
    private notificationService: NotificationService
  ) {}

  init(): Subscription {
    return this.summaryService.subscribe((summary) => {
      const current = new Map<string, ExecutingTask>();
      (summary.executing_tasks || [])
        .filter((task) => task.name?.startsWith(MIGRATION_TASK_PREFIX))
        .forEach((task) => current.set(task.name, task));

      this.runningMigrations.forEach((task, name) => {
        if (!current.has(name)) {
          this.notifyFinished(task);
        }
      });
      this.runningMigrations = current;
    });
  }

  private notifyFinished(task: ExecutingTask) {
    const { pool_name: poolName, src_name: srcName } = (task.metadata || {}) as Record<
      string,
      string
    >;
    this.notificationService.show(
      new CdNotificationConfig(
        NotificationType.success,
        $localize`Finished migrating pool '${poolName}' (source: ${srcName})`
      )
    );
  }
}
