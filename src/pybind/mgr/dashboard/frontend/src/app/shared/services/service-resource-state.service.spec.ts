import { TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
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
});
