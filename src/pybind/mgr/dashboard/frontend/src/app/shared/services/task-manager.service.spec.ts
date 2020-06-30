import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import * as _ from 'lodash';
import { BehaviorSubject } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { SummaryService } from './summary.service';
import { TaskManagerService } from './task-manager.service';

const summary: Record<string, any> = {
  executing_tasks: [],
  health_status: 'HEALTH_OK',
  mgr_id: 'x',
  rbd_mirroring: { errors: 0, warnings: 0 },
  rbd_pools: [],
  have_mon_connection: true,
  finished_tasks: [{ name: 'foo', metadata: {} }],
  filesystems: [{ id: 1, name: 'cephfs_a' }]
};

export class SummaryServiceMock {
  summaryDataSource = new BehaviorSubject(summary);
  summaryData$ = this.summaryDataSource.asObservable();

  refresh() {
    this.summaryDataSource.next(summary);
  }
  subscribe(call: any) {
    return this.summaryData$.subscribe(call);
  }
}

describe('TaskManagerService', () => {
  let taskManagerService: TaskManagerService;
  let summaryService: any;
  let called: boolean;

  configureTestBed({
    providers: [TaskManagerService, { provide: SummaryService, useClass: SummaryServiceMock }]
  });

  beforeEach(() => {
    taskManagerService = TestBed.inject(TaskManagerService);
    summaryService = TestBed.inject(SummaryService);
    called = false;
    taskManagerService.subscribe('foo', {}, () => (called = true));
  });

  it('should be created', () => {
    expect(taskManagerService).toBeTruthy();
  });

  it('should subscribe and be notified when task is finished', fakeAsync(() => {
    expect(taskManagerService.subscriptions.length).toBe(1);
    summaryService.refresh();
    tick();
    taskManagerService.init(summaryService);
    expect(called).toEqual(true);
    expect(taskManagerService.subscriptions).toEqual([]);
  }));

  it('should subscribe and process executing taks', fakeAsync(() => {
    const original_subscriptions = _.cloneDeep(taskManagerService.subscriptions);
    _.assign(summary, {
      executing_tasks: [{ name: 'foo', metadata: {} }],
      finished_tasks: []
    });
    summaryService.refresh();
    tick();
    expect(taskManagerService.subscriptions).toEqual(original_subscriptions);
  }));
});
