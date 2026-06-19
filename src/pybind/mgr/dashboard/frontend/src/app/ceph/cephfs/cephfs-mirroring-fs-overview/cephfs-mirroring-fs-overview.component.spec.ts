import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BehaviorSubject, of } from 'rxjs';
import { take } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import {
  Daemon,
  DaemonOverviewInfo,
  MirroringFsOverviewData,
  MirroringFsSyncInfo
} from '~/app/shared/models/cephfs.model';
import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';

import { CephfsMirroringFsOverviewComponent } from './cephfs-mirroring-fs-overview.component';

describe('CephfsMirroringFsOverviewComponent', () => {
  let component: CephfsMirroringFsOverviewComponent;
  let fixture: ComponentFixture<CephfsMirroringFsOverviewComponent>;
  let cephfsServiceMock: {
    listDaemonStatus: jest.Mock;
    listMirrorPeers: jest.Mock;
    getMirrorStatus: jest.Mock;
  };
  let refreshInterval$: BehaviorSubject<null>;

  const mockDaemonStatus: Daemon[] = [
    {
      daemon_id: 1,
      filesystems: [
        {
          filesystem_id: 1,
          id: '1',
          name: 'myfs',
          directory_count: 5,
          peers: [
            {
              uuid: 'peer-uuid',
              remote: {
                client_name: 'client.mirror',
                cluster_name: 'remote-cluster',
                fs_name: 'remote_fs',
                fsid: 'abc-123',
                mon_host: '10.0.0.1:6789'
              },
              stats: {
                failure_count: 2,
                recovery_count: 1
              }
            }
          ]
        }
      ]
    }
  ];

  const mockMirrorStatus = {
    metrics: {
      '/dir1': {
        peer: {
          'peer-uuid': {
            state: 'idle',
            last_synced_snap: {
              name: 'snap1',
              sync_bytes: '1.00 KiB',
              sync_time_stamp: '9000.000000s'
            },
            metrics_updated_at: 1_700_000_000
          }
        }
      },
      '/dir2': {
        peer: {
          'peer-uuid': {
            state: 'syncing'
          }
        }
      }
    }
  };

  beforeEach(async () => {
    refreshInterval$ = new BehaviorSubject<null>(null);
    cephfsServiceMock = {
      listDaemonStatus: jest.fn().mockReturnValue(of(mockDaemonStatus)),
      listMirrorPeers: jest.fn().mockReturnValue(
        of({
          'peer-uuid': {
            client_name: 'client.mirror',
            site_name: 'remote-site',
            fs_name: 'remote_fs'
          }
        })
      ),
      getMirrorStatus: jest.fn().mockReturnValue(of(mockMirrorStatus))
    };

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringFsOverviewComponent, RelativeDatePipe],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              paramMap: of(convertToParamMap({ fsName: 'myfs' }))
            }
          }
        },
        { provide: CephfsService, useValue: cephfsServiceMock },
        {
          provide: RefreshIntervalService,
          useValue: { intervalData$: refreshInterval$.asObservable() }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringFsOverviewComponent);
    component = fixture.componentInstance;

    fixture.detectChanges();
    return fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read fsName from parent route params', async () => {
    const fsData = await component.fsData$.pipe(take(1)).toPromise();
    expect(fsData?.fsName).toBe('myfs');
  });

  it('should populate fsData from daemon and mirror status', async () => {
    const fsData = await component.fsData$.pipe(take(1)).toPromise();

    expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalled();
    expect(cephfsServiceMock.listMirrorPeers).toHaveBeenCalledWith('myfs');
    expect(cephfsServiceMock.getMirrorStatus).toHaveBeenCalledWith('myfs', undefined, 'peer-uuid');
    expect(fsData?.stats.mirrorPaths).toBe(5);
    expect(fsData?.stats.failures).toBe(2);
    expect(fsData?.destination.clusterName).toBe('remote-cluster');
    expect(fsData?.destination.destinationFsName).toBe('remote_fs');
    expect(fsData?.destination.fsid).toBe('abc-123');
    expect(fsData?.destination.monitorEndpoint).toBe('10.0.0.1:6789');
    expect(fsData?.destination.siteName).toBe('remote-site');
    expect(fsData?.stats.syncingPaths).toBe(1);
    expect(fsData?.sync.bytesSynced).toBe('1.00 KiB');
    expect(fsData?.sync.path).toBe('/dir1');
    expect(fsData?.sync.snapName).toBe('snap1');
    expect(fsData?.sync.syncedAt).toBe(1_700_000_000);
  });

  it('should refresh overview data on interval tick', async () => {
    await component.fsData$.pipe(take(1)).toPromise();
    expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalledTimes(1);

    refreshInterval$.next(null);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalledTimes(2);
    expect(cephfsServiceMock.listMirrorPeers).toHaveBeenCalledTimes(2);
    expect(cephfsServiceMock.getMirrorStatus).toHaveBeenCalledTimes(2);
  });
});

describe('CephfsMirroringFsOverviewComponent helpers', () => {
  let component: CephfsMirroringFsOverviewComponent;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [CephfsMirroringFsOverviewComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: { parent: { paramMap: of(convertToParamMap({ fsName: 'myfs' })) } }
        },
        {
          provide: CephfsService,
          useValue: {
            listDaemonStatus: jest.fn(),
            listMirrorPeers: jest.fn(),
            getMirrorStatus: jest.fn()
          }
        },
        { provide: RefreshIntervalService, useValue: { intervalData$: of(null) } }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    });
    component = TestBed.createComponent(CephfsMirroringFsOverviewComponent).componentInstance;
  });

  const call = <T>(method: string, ...args: unknown[]): T =>
    ((component as unknown) as Record<string, (...a: unknown[]) => T>)[method](...args);

  it('getDaemonOverviewInfo returns defaults when filesystem is missing', () => {
    const info = call<DaemonOverviewInfo>('getDaemonOverviewInfo', [], 'missing');
    expect(info.mirrorPaths).toBe(0);
    expect(info.peerUuid).toBeUndefined();
  });

  it('getDaemonOverviewInfo maps daemon peer data', () => {
    const info = call<DaemonOverviewInfo>(
      'getDaemonOverviewInfo',
      [
        {
          filesystems: [
            {
              name: 'myfs',
              directory_count: 3,
              peers: [
                {
                  uuid: 'peer-1',
                  remote: { cluster_name: 'remote', fs_name: 'dest', fsid: 'id', mon_host: 'mon' },
                  stats: { failure_count: 1 }
                }
              ]
            }
          ]
        }
      ],
      'myfs'
    );

    expect(info.mirrorPaths).toBe(3);
    expect(info.failures).toBe(1);
    expect(info.peerUuid).toBe('peer-1');
    expect(info.clusterName).toBe('remote');
  });

  it('buildMirroringFsOverviewData uses empty sync when status is null', () => {
    const data = call<MirroringFsOverviewData>(
      'buildMirroringFsOverviewData',
      'myfs',
      {
        mirrorPaths: 2,
        failures: 0,
        clusterName: 'c',
        destinationFsName: 'd',
        fsid: 'f',
        monitorEndpoint: 'm',
        peerUuid: 'peer-1'
      },
      { 'peer-1': { site_name: 'site-a' } },
      null
    );

    expect(data.stats.syncingPaths).toBe(0);
    expect(data.destination.siteName).toBe('site-a');
    expect(data.sync.bytesSynced).toBe('-');
  });

  it('extractLatestSync counts syncing paths and picks latest snapshot', () => {
    const sync = call<{ syncingPaths: number; info: MirroringFsSyncInfo }>('extractLatestSync', {
      metrics: {
        '/old': {
          peer: {
            p1: {
              state: 'idle',
              last_synced_snap: { name: 'old', sync_bytes: '1 B', sync_time_stamp: '1s' },
              metrics_updated_at: 100
            }
          }
        },
        '/new': {
          peer: {
            p2: {
              state: 'syncing',
              last_synced_snap: { name: 'new', sync_bytes: '2 B', sync_time_stamp: '2s' },
              metrics_updated_at: 200
            }
          }
        }
      }
    });

    expect(sync.syncingPaths).toBe(1);
    expect(sync.info.snapName).toBe('new');
    expect(sync.info.path).toBe('/new');
    expect(sync.info.syncedAt).toBe(200);
  });

  it('isNewerMirrorSync prefers metrics_updated_at over sync timestamp', () => {
    expect(call<boolean>('isNewerMirrorSync', '1s', 300, '9s', 200)).toBe(true);
    expect(call<boolean>('isNewerMirrorSync', '9s', 100, '1s', 200)).toBe(false);
  });

  it('mirrorMetricsUpdatedAtToEpoch parses valid values and rejects invalid ones', () => {
    expect(call<number | null>('mirrorMetricsUpdatedAtToEpoch', 1_700_000_000.9)).toBe(
      1_700_000_000
    );
    expect(call<number | null>('mirrorMetricsUpdatedAtToEpoch', '1234.5')).toBe(1234);
    expect(call<number | null>('mirrorMetricsUpdatedAtToEpoch', '')).toBeNull();
    expect(call<number | null>('mirrorMetricsUpdatedAtToEpoch', 0)).toBeNull();
  });
});
