import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { Subject } from 'rxjs';

import { SummaryService } from './summary.service';
import { TaskManagerService } from './task-manager.service';

describe('TaskManagerService', () => {
  let taskManagerService: TaskManagerService;

  const summaryDataSource = new Subject();
  const fakeService = {
    summaryData$: summaryDataSource.asObservable()
  };

  const summary = {
    executing_tasks: [],
    health_status: 'HEALTH_OK',
    mgr_id: 'x',
    rbd_mirroring: { errors: 0, warnings: 0 },
    rbd_pools: [],
    have_mon_connection: true,
    finished_tasks: [{ name: 'foo', metadata: {} }],
    filesystems: [{ id: 1, name: 'cephfs_a' }]
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TaskManagerService, { provide: SummaryService, useValue: fakeService }]
    });

    taskManagerService = TestBed.get(TaskManagerService);
  });

  it('should be created', () => {
    expect(taskManagerService).toBeTruthy();
  });

  it(
    'should subscribe and be notified when task is finished',
    fakeAsync(() => {
      let called = false;
      taskManagerService.subscribe('foo', {}, () => (called = true));
      expect(taskManagerService.subscriptions.length).toBe(1);

      summaryDataSource.next(summary);
      tick();

      expect(called).toEqual(true);
      expect(taskManagerService.subscriptions).toEqual([]);
    })
  );

  it(
    'should subscribe and process executing taks',
    fakeAsync(() => {
      let called = false;
      taskManagerService.subscribe('foo', {}, () => (called = true));
      const original_subscriptions = _.cloneDeep(taskManagerService.subscriptions);
      _.assign(summary, {
        executing_tasks: [{ name: 'foo', metadata: {} }],
        finished_tasks: []
      });
      summaryDataSource.next(summary);
      tick();
      expect(taskManagerService.subscriptions).toEqual(original_subscriptions);
    })
  );
});
