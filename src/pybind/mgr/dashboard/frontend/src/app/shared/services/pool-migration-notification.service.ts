import { Injectable } from '@angular/core';

import { Subscription } from 'rxjs';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { ExecutingTask } from '../models/executing-task';
import { NotificationService } from './notification.service';
import { SummaryService } from './summary.service';

const MIGRATION_TASK_PREFIX = "progress/Migrating pool '";
/** Require the task to be absent for this many consecutive summary polls before notifying. */
const ABSENCE_POLLS_BEFORE_FINISH = 2;

/**
 * The migrate endpoint returns as soon as the mon command is accepted, long before the data
 * has moved. The real progress is reported by the backend as an executing task, so the
 * "finished" notification is emitted when that task is no longer reported.
 *
 * Disappearance is confirmed across consecutive summary polls so a transient osd_map that
 * omits migration_* fields does not produce a false "finished" toast.
 */
@Injectable({
  providedIn: 'root'
})
export class PoolMigrationNotificationService {
  private runningMigrations = new Map<string, ExecutingTask>();
  private missingPolls = new Map<string, number>();

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
        if (current.has(name)) {
          this.missingPolls.delete(name);
          return;
        }
        const misses = (this.missingPolls.get(name) || 0) + 1;
        if (misses >= ABSENCE_POLLS_BEFORE_FINISH) {
          this.notifyFinished(task);
          this.runningMigrations.delete(name);
          this.missingPolls.delete(name);
        } else {
          this.missingPolls.set(name, misses);
        }
      });

      current.forEach((task, name) => {
        this.runningMigrations.set(name, task);
        this.missingPolls.delete(name);
      });
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
