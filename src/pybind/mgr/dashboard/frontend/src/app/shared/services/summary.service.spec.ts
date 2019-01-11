import { HttpClient } from '@angular/common/http';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf, Subscriber } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ExecutingTask } from '../models/executing-task';
import { AuthStorageService } from './auth-storage.service';
import { SummaryService } from './summary.service';

describe('SummaryService', () => {
  let summaryService: SummaryService;
  let authStorageService: AuthStorageService;

  const summary = {
    executing_tasks: [],
    health_status: 'HEALTH_OK',
    mgr_id: 'x',
    rbd_mirroring: { errors: 0, warnings: 0 },
    rbd_pools: [],
    have_mon_connection: true,
    finished_tasks: [],
    filesystems: [{ id: 1, name: 'cephfs_a' }]
  };

  const httpClientSpy = {
    get: () => observableOf(summary)
  };

  configureTestBed({
    imports: [RouterTestingModule],
    providers: [
      SummaryService,
      AuthStorageService,
      { provide: HttpClient, useValue: httpClientSpy }
    ]
  });

  beforeEach(() => {
    summaryService = TestBed.get(SummaryService);
    authStorageService = TestBed.get(AuthStorageService);
  });

  it('should be created', () => {
    expect(summaryService).toBeTruthy();
  });

  it('should call refresh', fakeAsync(() => {
    summaryService.enablePolling();
    authStorageService.set('foobar', undefined, undefined);
    const calledWith = [];
    summaryService.subscribe((data) => {
      calledWith.push(data);
    });
    expect(calledWith).toEqual([summary]);
    summaryService.refresh();
    expect(calledWith).toEqual([summary, summary]);
    tick(10000);
    expect(calledWith.length).toEqual(4);
    // In order to not trigger setInterval again,
    // which would raise 'Error: 1 timer(s) still in the queue.'
    window.clearInterval(summaryService.polling);
  }));

  describe('Should test methods after first refresh', () => {
    beforeEach(() => {
      authStorageService.set('foobar', undefined, undefined);
      summaryService.refresh();
    });

    it('should call getCurrentSummary', () => {
      expect(summaryService.getCurrentSummary()).toEqual(summary);
    });

    it('should call subscribe', () => {
      let result;
      const subscriber = summaryService.subscribe((data) => {
        result = data;
      });
      expect(subscriber).toEqual(jasmine.any(Subscriber));
      expect(result).toEqual(summary);
    });

    it('should call addRunningTask', () => {
      summaryService.addRunningTask(
        new ExecutingTask('rbd/delete', {
          pool_name: 'somePool',
          image_name: 'someImage'
        })
      );
      const result = summaryService.getCurrentSummary();
      expect(result.executing_tasks.length).toBe(1);
      expect(result.executing_tasks[0]).toEqual({
        metadata: { image_name: 'someImage', pool_name: 'somePool' },
        name: 'rbd/delete'
      });
    });

    it('should call addRunningTask with duplicate task', () => {
      let result = summaryService.getCurrentSummary();
      const exec_task = new ExecutingTask('rbd/delete', {
        pool_name: 'somePool',
        image_name: 'someImage'
      });

      result.executing_tasks = [exec_task];
      summaryService['summaryDataSource'].next(result);
      result = summaryService.getCurrentSummary();
      expect(result.executing_tasks.length).toBe(1);

      summaryService.addRunningTask(exec_task);
      result = summaryService.getCurrentSummary();
      expect(result.executing_tasks.length).toBe(1);
    });
  });
});
