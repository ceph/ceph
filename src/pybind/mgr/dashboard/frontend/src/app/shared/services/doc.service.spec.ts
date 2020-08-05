import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { Subscriber } from 'rxjs';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { SharedModule } from '../shared.module';
import { DocService } from './doc.service';

describe('DocService', () => {
  let service: DocService;

  configureTestBed({ imports: [HttpClientTestingModule, SharedModule] });

  beforeEach(() => {
    service = TestBed.inject(DocService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should return full URL', () => {
    expect(service.urlGenerator('foo', 'iscsi')).toBe(
      'http://docs.ceph.com/docs/foo/mgr/dashboard/#enabling-iscsi-management'
    );
  });

  describe('Name of the group', () => {
    let result: string;
    let i: number;

    const nextSummary = (newData: any) => service['releaseDataSource'].next(newData);

    const callback = (response: string) => {
      i++;
      result = response;
    };

    beforeEach(() => {
      i = 0;
      result = undefined;
      nextSummary(undefined);
    });

    it('should call subscribeOnce without releaseName', () => {
      const subscriber = service.subscribeOnce('prometheus', callback);

      expect(subscriber).toEqual(jasmine.any(Subscriber));
      expect(i).toBe(0);
      expect(result).toEqual(undefined);
    });

    it('should call subscribeOnce with releaseName', () => {
      const subscriber = service.subscribeOnce('prometheus', callback);

      expect(subscriber).toEqual(jasmine.any(Subscriber));
      expect(i).toBe(0);
      expect(result).toEqual(undefined);

      nextSummary('foo');
      expect(result).toEqual(
        'http://docs.ceph.com/docs/foo/mgr/dashboard/#enabling-prometheus-alerting'
      );
      expect(i).toBe(1);
      expect(subscriber.closed).toBe(true);
    });
  });
});
