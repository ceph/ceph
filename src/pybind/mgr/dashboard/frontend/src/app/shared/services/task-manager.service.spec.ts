import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { Subject } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { SummaryService } from './summary.service';
import { TaskManagerService } from './task-manager.service';

describe('TaskManagerService', () => {
  let taskManagerService: TaskManagerService;
  let called: boolean;

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

  configureTestBed({
    providers: [TaskManagerService, { provide: SummaryService, useValue: fakeService }]
  }, true);

  beforeEach(() => {
    taskManagerService = TestBed.get(TaskManagerService);
    called = false;
    taskManagerService.subscribe('foo', {}, () => (called = true));
  });

  it('should be created', () => {
    expect(taskManagerService).toBeTruthy();
  });

  it(
    'should subscribe and be notified when task is finished',
    fakeAsync(() => {
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
