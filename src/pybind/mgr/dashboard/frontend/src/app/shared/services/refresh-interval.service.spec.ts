import { NgZone } from '@angular/core';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RefreshIntervalService } from './refresh-interval.service';

describe('RefreshIntervalService', () => {
  let service: RefreshIntervalService;

  configureTestBed({
    imports: [],
    providers: [RefreshIntervalService]
  });

  beforeEach(() => {
    service = TestBed.inject(RefreshIntervalService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should initial private interval time right', () => {
    sessionStorage.setItem('dashboard_interval', '10000');
    const ngZone = TestBed.inject(NgZone);
    service = new RefreshIntervalService(ngZone);
    expect(service.getRefreshInterval()).toBe(10000);
  });

  describe('setRefreshInterval', () => {
    let notifyCount: number;

    it('should send notification to component at correct interval time when interval changed', fakeAsync(() => {
      service.intervalData$.subscribe(() => {
        notifyCount++;
      });

      notifyCount = 0;
      service.setRefreshInterval(10000);
      tick(10000);
      expect(service.getRefreshInterval()).toBe(10000);
      expect(notifyCount).toBe(1);

      notifyCount = 0;
      service.setRefreshInterval(30000);
      tick(30000);
      expect(service.getRefreshInterval()).toBe(30000);
      expect(notifyCount).toBe(1);

      service.ngOnDestroy();
    }));
  });
});
