import { HttpClient } from '@angular/common/http';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { of as observableOf } from 'rxjs';

import { AuthStorageService } from './auth-storage.service';
import { SummaryService } from './summary.service';

describe('SummaryService', () => {
  let summaryService: SummaryService;
  let authStorageService: AuthStorageService;

  const httpClientSpy = {
    get: () =>
      observableOf({
        executing_tasks: [],
        health_status: 'HEALTH_OK',
        mgr_id: 'x',
        rbd_mirroring: { errors: 0, warnings: 0 },
        rbd_pools: [],
        have_mon_connection: true,
        finished_tasks: [],
        filesystems: [{ id: 1, name: 'cephfs_a' }]
      })
  };

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        SummaryService,
        AuthStorageService,
        { provide: HttpClient, useValue: httpClientSpy }
      ]
    });

    summaryService = TestBed.get(SummaryService);
    authStorageService = TestBed.get(AuthStorageService);
  });

  it('should be created', () => {
    expect(summaryService).toBeTruthy();
  });

  it(
    'should call refresh',
    fakeAsync(() => {
      authStorageService.set('foobar');
      let result = false;
      summaryService.refresh();
      summaryService.summaryData$.subscribe((res) => {
        result = true;
      });
      tick(5000);
      spyOn(summaryService, 'refresh').and.callFake(() => true);
      tick(5000);
      expect(result).toEqual(true);
    })
  );

  it('should get summary', () => {
    expect(summaryService.get()).toEqual(jasmine.any(Object));
  });
});
