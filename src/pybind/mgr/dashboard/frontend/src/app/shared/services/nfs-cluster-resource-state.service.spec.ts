import { TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { NfsClusterResourceStateService } from './nfs-cluster-resource-state.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { NFSCluster } from '../../ceph/nfs/models/nfs-cluster-config';

describe('NfsClusterResourceStateService', () => {
  let service: NfsClusterResourceStateService;
  let nfsServiceSpy: { nfsClusterList: jest.Mock };

  const clusters: NFSCluster[] = [
    {
      name: 'demo-nfs-cluster',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }]
    },
    {
      name: 'other-cluster',
      backend: []
    }
  ];

  beforeEach(() => {
    nfsServiceSpy = {
      nfsClusterList: jest.fn()
    };

    TestBed.configureTestingModule({
      providers: [NfsClusterResourceStateService, { provide: NfsService, useValue: nfsServiceSpy }]
    });

    service = TestBed.inject(NfsClusterResourceStateService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('load()', () => {
    it('should emit null when clusterIdRoute is empty', (done) => {
      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(cluster).toBeNull();
        expect(nfsServiceSpy.nfsClusterList).not.toHaveBeenCalled();
        done();
      });

      service.load('');
    });

    it('should emit the matching cluster on success', (done) => {
      nfsServiceSpy.nfsClusterList.mockReturnValue(of(clusters));

      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(nfsServiceSpy.nfsClusterList).toHaveBeenCalled();
        expect(cluster).toEqual(clusters[0]);
        done();
      });

      service.load('demo-nfs-cluster');
    });

    it('should emit null when cluster is not found', (done) => {
      nfsServiceSpy.nfsClusterList.mockReturnValue(of(clusters));

      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(cluster).toBeNull();
        done();
      });

      service.load('missing-cluster');
    });

    it('should emit null when nfsClusterList fails', (done) => {
      nfsServiceSpy.nfsClusterList.mockReturnValue(throwError(() => new Error('boom')));

      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(cluster).toBeNull();
        done();
      });

      service.load('demo-nfs-cluster');
    });

    it('should cancel in-flight requests when load is called again', () => {
      const firstRequest$ = new Subject<NFSCluster[]>();
      const secondRequest$ = new Subject<NFSCluster[]>();
      nfsServiceSpy.nfsClusterList
        .mockReturnValueOnce(firstRequest$.asObservable())
        .mockReturnValueOnce(secondRequest$.asObservable());

      const emitted: Array<string | null> = [];
      service.cluster$.subscribe((cluster) => {
        emitted.push(cluster?.name ?? null);
      });

      service.load('demo-nfs-cluster');
      service.load('other-cluster');

      secondRequest$.next([clusters[1]]);
      firstRequest$.next([clusters[0]]);

      expect(emitted).toEqual(['other-cluster']);
    });
  });
});
