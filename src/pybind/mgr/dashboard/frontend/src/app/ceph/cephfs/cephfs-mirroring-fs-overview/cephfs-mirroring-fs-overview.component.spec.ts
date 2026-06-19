import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BehaviorSubject, of } from 'rxjs';
import { take } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { Daemon } from '~/app/shared/models/cephfs.model';
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
            }
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
    expect(fsData?.sync.syncedAt).toBe(Math.floor(Date.now() / 1000 - 9000));
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
