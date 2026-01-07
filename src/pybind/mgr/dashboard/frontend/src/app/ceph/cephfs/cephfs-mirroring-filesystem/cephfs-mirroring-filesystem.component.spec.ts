import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { of, throwError } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Daemon, MirroringRow } from '~/app/shared/models/cephfs.model';
import { CephfsMirroringFilesystemComponent } from './cephfs-mirroring-filesystem.component';

describe('CephfsMirroringFilesystemComponent', () => {
  let component: CephfsMirroringFilesystemComponent;
  let fixture: ComponentFixture<CephfsMirroringFilesystemComponent>;

  const cephfsServiceMock = {
    listDaemonStatus: jest.fn()
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringFilesystemComponent],
      providers: [ActionLabelsI18n, { provide: CephfsService, useValue: cephfsServiceMock }]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringFilesystemComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should initialize columns correctly on ngOnInit', () => {
    component.ngOnInit();

    expect(component.columns.length).toBe(4);
    expect(component.columns[0].prop).toBe('remote_cluster_name');
  });

  it('should call loadDaemonStatus inside ngOnInit', () => {
    const loadSpy = jest.spyOn(component, 'loadDaemonStatus');
    component.ngOnInit();
    expect(loadSpy).toHaveBeenCalled();
  });

  it('should map daemon status to MirroringRow[] correctly', fakeAsync(() => {
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

    component.daemonStatus$.subscribe((v) => (emitted = v || []));
    component.ngOnInit();

    expect(emitted.length).toBe(1);
    expect(emitted[0]).toEqual({
      remote_cluster_name: 'clusterA',
      fs_name: 'fsA',
      client_name: 'clientA',
      directory_count: 3,
      id: '1-10'
    });
  }));

  it('should handle empty peers and map "-" values', fakeAsync(() => {
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

    component.daemonStatus$.subscribe((v) => (emitted = v || []));
    component.ngOnInit();

    expect(emitted.length).toBe(1);
    expect(emitted[0]).toEqual({
      remote_cluster_name: '-',
      fs_name: 'fs2',
      client_name: '-',
      directory_count: 5,
      peerId: '-',
      id: '2-20'
    });
  }));

  it('should throw error', fakeAsync(() => {
    const errorFn = jest.fn();
    component.context = { error: errorFn } as any;

    cephfsServiceMock.listDaemonStatus.mockReturnValue(throwError(() => new Error('error')));

    component.ngOnInit();
    component.daemonStatus$.subscribe(() => {});
    component.loadDaemonStatus();
    tick();

    expect(errorFn).toHaveBeenCalled();
  }));
});
