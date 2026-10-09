import { Injectable, OnDestroy } from '@angular/core';
import { of, ReplaySubject, Subject, Subscription } from 'rxjs';
import { catchError, map, switchMap } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { NFSCluster } from '../../ceph/nfs/models/nfs-cluster-config';

@Injectable()
export class NfsClusterResourceStateService implements OnDestroy {
  private clusterSource = new ReplaySubject<NFSCluster | null>(1);
  private clusterId$ = new Subject<string>();
  private loadSub: Subscription;

  readonly cluster$ = this.clusterSource.asObservable();

  constructor(private nfsService: NfsService) {
    this.loadSub = this.clusterId$
      .pipe(
        switchMap((clusterIdRoute) => {
          if (!clusterIdRoute) {
            return of(null);
          }
          try {
            const clusterId = decodeURIComponent(clusterIdRoute);
            return this.nfsService.nfsClusterList().pipe(
              map((clusters) => clusters.find((c) => c.name === clusterId) ?? null),
              catchError(() => of(null))
            );
          } catch {
            return of(null);
          }
        })
      )
      .subscribe((cluster) => this.clusterSource.next(cluster));
  }

  ngOnDestroy(): void {
    this.loadSub.unsubscribe();
  }

  load(clusterIdRoute: string): void {
    this.clusterId$.next(clusterIdRoute);
  }
}
