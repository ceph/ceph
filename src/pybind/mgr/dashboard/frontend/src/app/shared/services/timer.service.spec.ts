import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { of, Subscription } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { TimerService } from './timer.service';

describe('TimerService', () => {
  let service: TimerService;
  let subs: Subscription;
  let receivedData: any[];
  const next = () => of(true);
  const observer = (data: boolean) => {
    receivedData.push(data);
  };

  configureTestBed({
    providers: [TimerService]
  });

  beforeEach(() => {
    service = TestBed.inject(TimerService);
    receivedData = [];
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should not emit any value when no subscribers', fakeAsync(() => {
    subs = service.get(next).subscribe(observer);
    tick(service.DEFAULT_REFRESH_INTERVAL);
    expect(receivedData.length).toEqual(2);

    subs.unsubscribe();

    tick(service.DEFAULT_REFRESH_INTERVAL);
    expect(receivedData.length).toEqual(2);
  }));

  it('should emit value with no dueTime and no refresh interval', fakeAsync(() => {
    subs = service.get(next, null, null).subscribe(observer);
    tick(service.DEFAULT_REFRESH_INTERVAL);
    expect(receivedData.length).toEqual(1);
    expect(receivedData).toEqual([true]);

    subs.unsubscribe();
  }));

  it('should emit expected values when refresh interval + no dueTime', fakeAsync(() => {
    subs = service.get(next).subscribe(observer);
    tick(service.DEFAULT_REFRESH_INTERVAL * 2);
    expect(receivedData.length).toEqual(3);
    expect(receivedData).toEqual([true, true, true]);

    subs.unsubscribe();
  }));

  it('should emit expected values when dueTime equal to refresh interval', fakeAsync(() => {
    const dueTime = 1000;
    subs = service.get(next, service.DEFAULT_REFRESH_INTERVAL, dueTime).subscribe(observer);
    tick(service.DEFAULT_REFRESH_INTERVAL * 2);
    expect(receivedData.length).toEqual(2);
    expect(receivedData).toEqual([true, true]);

    subs.unsubscribe();
  }));
});
