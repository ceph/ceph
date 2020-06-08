import { HttpClient } from '@angular/common/http';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { of as observableOf, Subscriber, Subscription } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ExecutingTask } from '../models/executing-task';
import { Summary } from '../models/summary.model';
import { AuthStorageService } from './auth-storage.service';
import { SummaryService } from './summary.service';

describe('SummaryService', () => {
  let summaryService: SummaryService;
  let authStorageService: AuthStorageService;
  let subs: Subscription;

  const summary: Summary = {
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

  const nextSummary = (newData: any) => summaryService['summaryDataSource'].next(newData);

  configureTestBed({
    imports: [RouterTestingModule],
    providers: [
      SummaryService,
      AuthStorageService,
      { provide: HttpClient, useValue: httpClientSpy }
    ]
  });

  beforeEach(() => {
    summaryService = TestBed.inject(SummaryService);
    authStorageService = TestBed.inject(AuthStorageService);
  });

  it('should be created', () => {
    expect(summaryService).toBeTruthy();
  });

  it('should call refresh', fakeAsync(() => {
    authStorageService.set('foobar', undefined, undefined);
    const calledWith: any[] = [];
    subs = new Subscription();
    subs.add(summaryService.startPolling());
    tick();
    subs.add(
      summaryService.subscribe((data) => {
        calledWith.push(data);
      })
    );
    expect(calledWith).toEqual([summary]);
    subs.add(summaryService.refresh());
    expect(calledWith).toEqual([summary, summary]);
    tick(summaryService.REFRESH_INTERVAL * 2);
    expect(calledWith.length).toEqual(4);
    subs.unsubscribe();
  }));

  describe('Should test subscribe without initial value', () => {
    let result: Summary;
    let i: number;

    const callback = (response: Summary) => {
      i++;
      result = response;
    };

    beforeEach(() => {
      i = 0;
      result = undefined;
      nextSummary(undefined);
    });

    it('should call subscribeOnce', () => {
      const subscriber = summaryService.subscribeOnce(callback);

      expect(subscriber).toEqual(jasmine.any(Subscriber));
      expect(i).toBe(0);
      expect(result).toEqual(undefined);

      nextSummary(undefined);
      expect(i).toBe(0);
      expect(result).toEqual(undefined);
      expect(subscriber.closed).toBe(false);

      nextSummary(summary);
      expect(result).toEqual(summary);
      expect(i).toBe(1);
      expect(subscriber.closed).toBe(true);

      nextSummary(summary);
      expect(result).toEqual(summary);
      expect(i).toBe(1);
    });

    it('should call subscribe', () => {
      const subscriber = summaryService.subscribe(callback);

      expect(subscriber).toEqual(jasmine.any(Subscriber));
      expect(i).toBe(0);
      expect(result).toEqual(undefined);

      nextSummary(undefined);
      expect(i).toBe(0);
      expect(result).toEqual(undefined);
      expect(subscriber.closed).toBe(false);

      nextSummary(summary);
      expect(result).toEqual(summary);
      expect(i).toBe(1);
      expect(subscriber.closed).toBe(false);

      nextSummary(summary);
      expect(result).toEqual(summary);
      expect(i).toBe(2);
      expect(subscriber.closed).toBe(false);
    });
  });

  describe('Should test methods after first refresh', () => {
    beforeEach(() => {
      authStorageService.set('foobar', undefined, undefined);
      summaryService.refresh();
    });

    it('should call addRunningTask', () => {
      summaryService.addRunningTask(
        new ExecutingTask('rbd/delete', {
          pool_name: 'somePool',
          image_name: 'someImage'
        })
      );
      let result: any;
      summaryService.subscribeOnce((response) => {
        result = response;
      });

      expect(result.executing_tasks.length).toBe(1);
      expect(result.executing_tasks[0]).toEqual({
        metadata: { image_name: 'someImage', pool_name: 'somePool' },
        name: 'rbd/delete'
      });
    });

    it('should call addRunningTask with duplicate task', () => {
      let result: any;
      summaryService.subscribe((response) => {
        result = response;
      });

      const exec_task = new ExecutingTask('rbd/delete', {
        pool_name: 'somePool',
        image_name: 'someImage'
      });

      result.executing_tasks = [exec_task];
      nextSummary(result);

      expect(result.executing_tasks.length).toBe(1);

      summaryService.addRunningTask(exec_task);

      expect(result.executing_tasks.length).toBe(1);
    });
  });
});
