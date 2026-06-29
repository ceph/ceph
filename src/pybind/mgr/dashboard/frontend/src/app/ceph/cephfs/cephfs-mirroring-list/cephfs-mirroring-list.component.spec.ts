import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { of } from 'rxjs';

import { CephfsMirroringListComponent } from './cephfs-mirroring-list.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import {
  Daemon,
  hasPendingReplication,
  MirroringRow,
  MirrorStatusResponse
} from '~/app/shared/models/cephfs.model';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { MirroringSyncStatus } from '~/app/shared/enum/cephfs-mirroring-sync-status.enum';

describe('hasPendingReplication', () => {
  const peerUuid = 'peer-uuid';

  it('should return true when mirror status reports an active sync', () => {
    const status: MirrorStatusResponse = {
      metrics: {
        '/dir': {
          peer: {
            [peerUuid]: { state: MirroringSyncStatus.SYNCING }
          }
        }
      }
    };
    expect(hasPendingReplication(status, peerUuid)).toBe(true);
  });

  it('should return true when mirror status reports a current syncing snapshot', () => {
    const status: MirrorStatusResponse = {
      metrics: {
        '/dir': {
          peer: {
            [peerUuid]: { state: 'idle', current_syncing_snap: { name: 'snap1' } }
          }
        }
      }
    };
    expect(hasPendingReplication(status, peerUuid)).toBe(true);
  });

  it('should return false when idle with no active sync', () => {
    const status: MirrorStatusResponse = {
      metrics: {
        '/dir': {
          peer: {
            [peerUuid]: { state: 'idle' }
          }
        }
      }
    };
    expect(hasPendingReplication(status, peerUuid)).toBe(false);
  });

  it('should return false when status is unavailable', () => {
    expect(hasPendingReplication(null, peerUuid)).toBe(false);
    expect(hasPendingReplication({ metrics: {} }, undefined)).toBe(false);
  });
});

describe('CephfsMirroringListComponent', () => {
  let component: CephfsMirroringListComponent;
  let fixture: ComponentFixture<CephfsMirroringListComponent>;
  let routerNavigateSpy: jest.Mock;

  const cephfsServiceMock = {
    listDaemonStatus: jest.fn(),
    disableMirror: jest.fn(),
    getMirrorStatus: jest.fn()
  };
  const modalServiceMock = {
    show: jest.fn()
  };
  const taskWrapperMock = {
    wrapTaskAroundCall: jest.fn()
  };

  const authStorageServiceMock = {
    getPermissions: jest.fn().mockReturnValue({ cephfsMirror: {} as Permission })
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    routerNavigateSpy = jest.fn();

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringListComponent],
      providers: [
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: AuthStorageService, useValue: authStorageServiceMock },
        { provide: ModalCdsService, useValue: modalServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperMock },
        {
          provide: Router,
          useValue: {
            navigate: routerNavigateSpy,
            url: '/cephfs/mirroring',
            events: of()
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringListComponent);
    component = fixture.componentInstance;
  });

  it('should initialize columns correctly on ngOnInit', () => {
    cephfsServiceMock.listDaemonStatus.mockReturnValue(of([]));
    component.ngOnInit();

    expect(component.jumpInTiles.length).toBe(2);
    expect(component.columns.length).toBe(6);
    expect(component.columns[0].prop).toBe('local_fs_name');
  });

  it('should load daemon status on ngOnInit', () => {
    cephfsServiceMock.listDaemonStatus.mockReturnValue(of([]));
    component.daemonStatus$.subscribe();
    component.ngOnInit();
    expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalledTimes(1);
  });

  it('should fetch daemon status when loadDaemonStatus() is called', () => {
    cephfsServiceMock.listDaemonStatus.mockReturnValue(of([]));
    component.daemonStatus$.subscribe();
    component.loadDaemonStatus();
    expect(cephfsServiceMock.listDaemonStatus).toHaveBeenCalledTimes(1);
  });

  it('should map daemon status to MirroringRow[] correctly', () => {
    const mockData: Daemon[] = [
      {
        daemon_id: 1,
        filesystems: [
          {
            filesystem_id: 10,
            name: 'fs1',
            directory_count: 3,
            peers: [
              {
                remote: {
                  cluster_name: 'clusterA',
                  fs_name: 'fsA',
                  client_name: 'clientA'
                },
                uuid: 'peer-uuid',
                stats: { failure_count: 0, recovery_count: 1 }
              }
            ],
            id: ''
          }
        ]
      }
    ];

    cephfsServiceMock.listDaemonStatus.mockReturnValue(of(mockData));

    let emitted: MirroringRow[] = [];

    component.ngOnInit();
    component.daemonStatus$.subscribe((v) => (emitted = v || []));
    component.loadDaemonStatus();

    expect(emitted.length).toBe(1);
    expect(emitted[0]).toEqual({
      remote_cluster_name: 'clusterA',
      local_fs_name: 'fs1',
      fs_name: 'fsA',
      client_name: 'clientA',
      directory_count: 3,
      filesystem_id: 10,
      peer_uuid: 'peer-uuid',
      failure_count: 0,
      recovery_count: 1,
      sync_status: MirroringSyncStatus.SYNCING,
      sync_status_label: 'Syncing',
      id: '1-10'
    });
  });

  it('should handle empty peers and map "-" values', () => {
    const mockData: Daemon[] = [
      {
        daemon_id: 2,
        filesystems: [
          {
            filesystem_id: 20,
            name: 'fs2',
            directory_count: 5,
            peers: [],
            id: ''
          }
        ]
      }
    ];

    cephfsServiceMock.listDaemonStatus.mockReturnValue(of(mockData));

    let emitted: MirroringRow[] = [];

    component.ngOnInit();
    component.daemonStatus$.subscribe((v) => (emitted = v || []));
    component.loadDaemonStatus();

    expect(emitted.length).toBe(1);
    expect(emitted[0]).toEqual({
      remote_cluster_name: '-',
      local_fs_name: 'fs2',
      fs_name: 'fs2',
      client_name: '-',
      directory_count: 5,
      filesystem_id: 20,
      peerId: '-',
      failure_count: 0,
      recovery_count: 0,
      sync_status: MirroringSyncStatus.NONE,
      sync_status_label: '-',
      id: '2-20'
    });
  });

  it('should not navigate to add path modal when filesystem_id is missing', () => {
    component.selection = new CdTableSelection([{ local_fs_name: 'fs1' } as MirroringRow]);

    component.openAddPath();

    expect(routerNavigateSpy).not.toHaveBeenCalled();
  });

  it('should navigate to add path modal outlet when a filesystem is selected', () => {
    component.selection = new CdTableSelection([
      { local_fs_name: 'fs1', filesystem_id: 10 } as MirroringRow
    ]);

    component.openAddPath();

    expect(routerNavigateSpy).toHaveBeenCalledWith([
      '/cephfs/mirroring',
      {
        outlets: {
          modal: ['add-path', 10, encodeURIComponent('fs1')]
        }
      }
    ]);
  });

  it('should open disable mirroring confirmation modal with active sync warning', () => {
    cephfsServiceMock.getMirrorStatus.mockReturnValue(
      of({
        metrics: {
          '/dir': {
            peer: {
              'peer-uuid': { state: MirroringSyncStatus.SYNCING }
            }
          }
        }
      })
    );

    component.ngOnInit();
    component.selection.selected = [
      {
        local_fs_name: 'fs1',
        remote_cluster_name: 'clusterA',
        directory_count: 4,
        peer_uuid: 'peer-uuid',
        failure_count: 0,
        recovery_count: 0,
        sync_status: MirroringSyncStatus.SYNCING,
        sync_status_label: 'Syncing'
      } as MirroringRow
    ];

    component.disableMirroringModal();

    expect(cephfsServiceMock.getMirrorStatus).toHaveBeenCalledWith('fs1', undefined, 'peer-uuid');
    expect(modalServiceMock.show).toHaveBeenCalledWith(
      DeleteConfirmationModalComponent,
      expect.objectContaining({
        impact: DeletionImpact.high,
        itemNames: ['fs1'],
        actionDescription: 'disable',
        submitText: 'Disable',
        bodyContext: expect.objectContaining({
          hasPendingReplication: true
        })
      })
    );
  });
});
