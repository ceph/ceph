import { TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { ServiceResourceStateService } from './service-resource-state.service';

describe('ServiceResourceStateService', () => {
  let service: ServiceResourceStateService;
  let cephServiceServiceSpy: any;

  beforeEach(() => {
    cephServiceServiceSpy = {
      list: jest.fn()
    };

    TestBed.configureTestingModule({
      providers: [
        ServiceResourceStateService,
        { provide: CephServiceService, useValue: cephServiceServiceSpy }
      ]
    });

    service = TestBed.inject(ServiceResourceStateService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should emit null when route parameter is empty', (done) => {
    service.service$.pipe(take(1)).subscribe((selected) => {
      expect(selected).toBeNull();
      done();
    });

    service.load('');
  });

  it('should emit selected service when API succeeds', (done) => {
    cephServiceServiceSpy.list.mockReturnValue({
      observable: of([{ service_name: 'mgr' }, { service_name: 'mds' }])
    });

    service.service$.pipe(take(1)).subscribe((selected) => {
      expect(cephServiceServiceSpy.list).toHaveBeenCalled();
      expect(selected?.service_name).toBe('mds');
      done();
    });

    service.load('mds');
  });

  it('should emit null on API error', (done) => {
    cephServiceServiceSpy.list.mockReturnValue({
      observable: throwError(() => new Error('failed'))
    });

    service.service$.pipe(take(1)).subscribe((selected) => {
      expect(selected).toBeNull();
      done();
    });

    service.load('mds');
  });

  it('should ignore stale responses from previous load calls', () => {
    const firstRequest$ = new Subject<any>();
    const secondRequest$ = new Subject<any>();
    const selectedServices: string[] = [];

    cephServiceServiceSpy.list
      .mockReturnValueOnce({ observable: firstRequest$ })
      .mockReturnValueOnce({ observable: secondRequest$ });

    service.service$.subscribe((selected) => {
      if (selected?.service_name) {
        selectedServices.push(selected.service_name);
      }
    });

    service.load('mgr');
    service.load('mds');

    secondRequest$.next([{ service_name: 'mds' }]);
    expect(selectedServices).toEqual(['mds']);

    firstRequest$.next([{ service_name: 'mgr' }]);
    expect(selectedServices).toEqual(['mds']);
  });
});
