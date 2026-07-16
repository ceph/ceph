import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  inject,
  ViewEncapsulation
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, convertToParamMap, ParamMap } from '@angular/router';
import { EMPTY, forkJoin, Observable, of } from 'rxjs';
import { catchError, exhaustMap, map, shareReplay, switchMap } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import {
  Daemon,
  DaemonOverviewInfo,
  MirroringFsOverviewData,
  MirrorPeerList,
  MirrorStatusResponse
} from '~/app/shared/models/cephfs.model';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { IconSize } from '~/app/shared/enum/icons.enum';
import { MirroringSyncUtils } from '../mirroring-sync-utils';

@Component({
  selector: 'cd-cephfs-mirroring-fs-overview',
  templateUrl: './cephfs-mirroring-fs-overview.component.html',
  styleUrls: ['./cephfs-mirroring-fs-overview.component.scss'],
  standalone: false,
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None
})
export class CephfsMirroringFsOverviewComponent {
  private readonly route = inject(ActivatedRoute);
  private readonly cephfsService = inject(CephfsService);
  private readonly refreshIntervalService = inject(RefreshIntervalService);
  private readonly destroyRef = inject(DestroyRef);

  readonly iconSize = IconSize;

  private readonly fsName$ = (this.route.parent?.paramMap ?? of(convertToParamMap({}))).pipe(
    map((paramMap: ParamMap) => paramMap.get('fsName') ?? '-')
  );

  readonly fsData$ = this.fsName$.pipe(
    switchMap((fsName) => this.refreshIntervalObs(() => this.fetchFsData(fsName))),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  private refreshIntervalObs<T>(fn: () => Observable<T>): Observable<T> {
    return this.refreshIntervalService.intervalData$.pipe(
      exhaustMap(() => fn().pipe(catchError(() => EMPTY))),
      takeUntilDestroyed(this.destroyRef)
    );
  }

  private fetchFsData(fsName: string): Observable<MirroringFsOverviewData> {
    return forkJoin({
      daemons: this.cephfsService.listDaemonStatus().pipe(catchError(() => of([] as Daemon[]))),
      peers: this.cephfsService
        .listMirrorPeers(fsName)
        .pipe(catchError(() => of({} as MirrorPeerList)))
    }).pipe(
      switchMap(({ daemons, peers }) => {
        const daemonInfo = this.getDaemonOverviewInfo(daemons, fsName);
        if (!daemonInfo.peerUuid) {
          return of(this.buildMirroringFsOverviewData(fsName, daemonInfo, peers, null));
        }

        return this.cephfsService.getMirrorStatus(fsName, undefined, daemonInfo.peerUuid).pipe(
          catchError(() => of({} as MirrorStatusResponse)),
          map((status) => this.buildMirroringFsOverviewData(fsName, daemonInfo, peers, status))
        );
      })
    );
  }

  private getDaemonOverviewInfo(daemons: Daemon[], fsName: string): DaemonOverviewInfo {
    const empty: DaemonOverviewInfo = {
      mirrorPaths: 0,
      failures: 0,
      clusterName: '-',
      destinationFsName: '-',
      fsid: '-',
      monitorEndpoint: '-'
    };

    for (const daemon of daemons) {
      const fs = daemon.filesystems?.find((filesystem) => filesystem.name === fsName);
      if (!fs) {
        continue;
      }

      const peer = fs.peers?.[0];
      return {
        mirrorPaths: fs.directory_count ?? 0,
        failures: (fs.peers ?? []).reduce((sum, item) => sum + (item.stats?.failure_count ?? 0), 0),
        clusterName: peer?.remote?.cluster_name ?? '-',
        destinationFsName: peer?.remote?.fs_name ?? '-',
        fsid: peer?.remote?.fsid ?? '-',
        monitorEndpoint: peer?.remote?.mon_host ?? '-',
        peerUuid: peer?.uuid
      };
    }

    return empty;
  }

  private buildMirroringFsOverviewData(
    fsName: string,
    daemonInfo: DaemonOverviewInfo,
    peers: MirrorPeerList,
    status: MirrorStatusResponse | null
  ): MirroringFsOverviewData {
    const sync = status ? MirroringSyncUtils.extractLatestSync(status) : { syncingPaths: 0, info: MirroringSyncUtils.emptySyncInfo() };

    return {
      fsName,
      stats: {
        mirrorPaths: daemonInfo.mirrorPaths,
        failures: daemonInfo.failures,
        syncingPaths: sync.syncingPaths
      },
      destination: {
        clusterName: daemonInfo.clusterName,
        siteName: daemonInfo.peerUuid ? peers[daemonInfo.peerUuid]?.site_name ?? '-' : '-',
        destinationFsName: daemonInfo.destinationFsName,
        fsid: daemonInfo.fsid,
        monitorEndpoint: daemonInfo.monitorEndpoint
      },
      sync: sync.info
    };
  }
}
