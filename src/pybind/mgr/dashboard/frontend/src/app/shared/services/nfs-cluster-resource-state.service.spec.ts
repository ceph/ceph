import { TestBed } from '@angular/core/testing';
import { of, Subject, throwError } from 'rxjs';
import { take } from 'rxjs/operators';

import { NfsClusterResourceStateService } from './nfs-cluster-resource-state.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { NFSCluster } from '../../ceph/nfs/models/nfs-cluster-config';

describe('NfsClusterResourceStateService', () => {
  let service: NfsClusterResourceStateService;
  let nfsServiceSpy: { getCluster: jest.Mock };

  const clusters: NFSCluster[] = [
    {
      name: 'demo-nfs-cluster',
      virtual_ip: null,
      backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }]
    },
    {
      name: 'other-cluster',
      virtual_ip: null,
      backend: []
    }
  ];

  beforeEach(() => {
    nfsServiceSpy = {
      getCluster: jest.fn()
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
        expect(nfsServiceSpy.getCluster).not.toHaveBeenCalled();
        done();
      });

      service.load('');
    });

    it('should emit the matching cluster on success', (done) => {
      nfsServiceSpy.getCluster.mockReturnValue(of(clusters[0]));

      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(nfsServiceSpy.getCluster).toHaveBeenCalledWith('demo-nfs-cluster');
        expect(cluster).toEqual(clusters[0]);
        done();
      });

      service.load('demo-nfs-cluster');
    });

    it('should emit null when cluster is not found', (done) => {
      nfsServiceSpy.getCluster.mockReturnValue(throwError(() => new Error('not found')));

      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(cluster).toBeNull();
        done();
      });

      service.load('missing-cluster');
    });

    it('should emit null when getCluster fails', (done) => {
      nfsServiceSpy.getCluster.mockReturnValue(throwError(() => new Error('boom')));

      service.cluster$.pipe(take(1)).subscribe((cluster) => {
        expect(cluster).toBeNull();
        done();
      });

      service.load('demo-nfs-cluster');
    });

    it('should cancel in-flight requests when load is called again', () => {
      const firstRequest$ = new Subject<NFSCluster>();
      const secondRequest$ = new Subject<NFSCluster>();
      nfsServiceSpy.getCluster
        .mockReturnValueOnce(firstRequest$.asObservable())
        .mockReturnValueOnce(secondRequest$.asObservable());

      const emitted: Array<string | null> = [];
      service.cluster$.subscribe((cluster) => {
        emitted.push(cluster?.name ?? null);
      });

      service.load('demo-nfs-cluster');
      service.load('other-cluster');

      secondRequest$.next(clusters[1]);
      firstRequest$.next(clusters[0]);

      expect(emitted).toEqual(['other-cluster']);
    });
  });
});
