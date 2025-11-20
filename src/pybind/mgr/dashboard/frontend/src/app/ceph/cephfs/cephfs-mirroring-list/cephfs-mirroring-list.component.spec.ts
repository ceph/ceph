import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';

import { CephfsMirroringListComponent } from './cephfs-mirroring-list.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Daemon, MirroringRow } from '~/app/shared/models/cephfs.model';

describe('CephfsMirroringListComponent', () => {
  let component: CephfsMirroringListComponent;
  let fixture: ComponentFixture<CephfsMirroringListComponent>;

  const cephfsServiceMock = {
    listDaemonStatus: jest.fn()
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringListComponent],
      providers: [ActionLabelsI18n, { provide: CephfsService, useValue: cephfsServiceMock }]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringListComponent);
    component = fixture.componentInstance;
  });

  it('should initialize columns correctly on ngOnInit', () => {
    component.ngOnInit();

    expect(component.columns.length).toBe(5);
    expect(component.columns[0].prop).toBe('remote_cluster_name');
  });

  it('should call loadDaemonStatus inside ngOnInit', () => {
    const loadSpy = jest.spyOn(component, 'loadDaemonStatus');
    component.ngOnInit();
    expect(loadSpy).toHaveBeenCalledTimes(1);
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
                uuid: '',
                stats: undefined
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
      peerId: '-',
      id: '2-20'
    });
  });
});
