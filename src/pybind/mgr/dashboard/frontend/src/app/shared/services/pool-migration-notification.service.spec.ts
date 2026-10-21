import { TestBed } from '@angular/core/testing';

import { BehaviorSubject } from 'rxjs';

import { NotificationType } from '../enum/notification-type.enum';
import { Summary } from '../models/summary.model';
import { NotificationService } from './notification.service';
import { PoolMigrationNotificationService } from './pool-migration-notification.service';
import { SummaryService } from './summary.service';

describe('PoolMigrationNotificationService', () => {
  let service: PoolMigrationNotificationService;
  let summary$: BehaviorSubject<Summary>;
  let notificationService: { show: jest.Mock };

  const migrationTask = (poolName: string, srcName: string) => ({
    name: `progress/Migrating pool '${poolName}' (source: ${srcName})`,
    description: '',
    metadata: { pool_name: poolName, pool_id: 2, src_name: srcName },
    begin_time: 0,
    progress: 10
  });

  beforeEach(() => {
    summary$ = new BehaviorSubject<Summary>({ executing_tasks: [], finished_tasks: [] });
    notificationService = { show: jest.fn() };
    TestBed.configureTestingModule({
      providers: [
        PoolMigrationNotificationService,
        { provide: NotificationService, useValue: notificationService },
        {
          provide: SummaryService,
          useValue: { subscribe: (next: (s: Summary) => void) => summary$.subscribe(next) }
        }
      ]
    });
    service = TestBed.inject(PoolMigrationNotificationService);
    service.init();
  });

  it('should not notify while a migration is running', () => {
    summary$.next({ executing_tasks: [migrationTask('target', '.migrate-1')] });
    summary$.next({ executing_tasks: [migrationTask('target', '.migrate-1')] });

    expect(notificationService.show).not.toHaveBeenCalled();
  });

  it('should notify with pool names once the migration task disappears', () => {
    summary$.next({ executing_tasks: [migrationTask('target', '.migrate-1')] });
    summary$.next({ executing_tasks: [] });

    expect(notificationService.show).toHaveBeenCalledTimes(1);
    const config = notificationService.show.mock.calls[0][0];
    expect(config.type).toBe(NotificationType.success);
    expect(config.title).toBe("Finished migrating pool 'target' (source: .migrate-1)");
  });

  it('should notify only once per finished migration', () => {
    summary$.next({ executing_tasks: [migrationTask('target', '.migrate-1')] });
    summary$.next({ executing_tasks: [] });
    summary$.next({ executing_tasks: [] });

    expect(notificationService.show).toHaveBeenCalledTimes(1);
  });

  it('should track several migrations independently', () => {
    summary$.next({
      executing_tasks: [migrationTask('a', '.migrate-1'), migrationTask('b', '.migrate-2')]
    });
    summary$.next({ executing_tasks: [migrationTask('b', '.migrate-2')] });

    expect(notificationService.show).toHaveBeenCalledTimes(1);
    expect(notificationService.show.mock.calls[0][0].title).toContain("'a'");
  });

  it('should ignore other progress tasks', () => {
    summary$.next({
      executing_tasks: [
        { name: 'progress/Upgrade', description: '', metadata: {}, begin_time: 0, progress: 1 }
      ]
    });
    summary$.next({ executing_tasks: [] });

    expect(notificationService.show).not.toHaveBeenCalled();
  });
});
