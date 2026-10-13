import { Injectable, OnDestroy } from '@angular/core';
import { forkJoin, of, ReplaySubject, Subject, Subscription } from 'rxjs';
import { catchError, switchMap } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { NFSBwIopConfig, NFSCluster } from '../../ceph/nfs/models/nfs-cluster-config';

export interface NfsClusterState {
  cluster: NFSCluster | null;
  qos: NFSBwIopConfig | null;
}

@Injectable()
export class NfsClusterResourceStateService implements OnDestroy {
  private stateSource = new ReplaySubject<NfsClusterState>(1);
  private clusterId$ = new Subject<string>();
  private loadSub: Subscription;

  readonly state$ = this.stateSource.asObservable();

  /** Convenience observable — emits the cluster portion only. */
  readonly cluster$ = new ReplaySubject<NFSCluster | null>(1);

  constructor(private nfsService: NfsService) {
    this.loadSub = this.clusterId$
      .pipe(
        switchMap((clusterIdRoute) => {
          if (!clusterIdRoute) {
            return of({ cluster: null, qos: null });
          }

          try {
            const clusterId = decodeURIComponent(clusterIdRoute);
            return forkJoin({
              cluster: this.nfsService.getCluster(clusterId).pipe(catchError(() => of(null))),
              qos: this.nfsService
                .getClusterBandwidthOpsConfig(clusterId)
                .pipe(catchError(() => of(null)))
            });
          } catch {
            return of({ cluster: null, qos: null });
          }
        })
      )
      .subscribe((state) => {
        this.stateSource.next(state as NfsClusterState);
        this.cluster$.next((state as NfsClusterState).cluster);
      });
  }

  ngOnDestroy(): void {
    this.loadSub.unsubscribe();
  }

  load(clusterIdRoute: string): void {
    this.clusterId$.next(clusterIdRoute);
  }
}
