import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { of, throwError } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CephfsFilesystemSelectorComponent } from './cephfs-filesystem-selector.component';

const createDetail = (
  id: number,
  name: string,
  pools: Array<{ pool: string; used: number }>,
  enabled = true,
  peers: Record<string, unknown> = { peer: {} }
) => ({
  cephfs: {
    id,
    name,
    pools,
    flags: { enabled },
    mirror_info: { peers }
  }
});

describe('CephfsFilesystemSelectorComponent', () => {
  let component: CephfsFilesystemSelectorComponent;
  let fixture: ComponentFixture<CephfsFilesystemSelectorComponent>;
  let cephfsServiceMock: jest.Mocked<Pick<CephfsService, 'list' | 'getCephfs'>>;

  beforeEach(async () => {
    cephfsServiceMock = {
      list: jest.fn(),
      getCephfs: jest.fn()
    };

    cephfsServiceMock.list.mockReturnValue(of([]));
    cephfsServiceMock.getCephfs.mockReturnValue(of(null));

    await TestBed.configureTestingModule({
      declarations: [CephfsFilesystemSelectorComponent],
      providers: [{ provide: CephfsService, useValue: cephfsServiceMock }, DimlessBinaryPipe],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsFilesystemSelectorComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should configure columns on init', () => {
    fixture.detectChanges();

    expect(component.columns.map((c) => c.prop)).toEqual([
      'name',
      'used',
      'pools',
      'mdsStatus',
      'mirroringStatus'
    ]);
  });

  it('should populate filesystems from service data', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(of([{ id: 1 }]));
    cephfsServiceMock.getCephfs.mockReturnValue(
      of(
        createDetail(1, 'fs1', [
          { pool: 'data', used: 100 },
          { pool: 'meta', used: 50 }
        ])
      )
    );

    fixture.detectChanges();

    let filesystems: any[] = [];
    component.filesystems$.subscribe((rows) => {
      filesystems = rows;
    });
    tick();

    expect(filesystems).toEqual([
      {
        id: 1,
        name: 'fs1',
        pools: ['data', 'meta'],
        used: '150',
        mdsStatus: 'Inactive',
        mirroringStatus: 'Disabled'
      }
    ]);
  }));

  it('should set mirroring status to Disabled when list response has no mirror info', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(of([{ id: 2 }]));
    cephfsServiceMock.getCephfs.mockReturnValue(of(createDetail(2, 'fs2', [])));

    fixture.detectChanges();

    let filesystems: any[] = [];
    component.filesystems$.subscribe((rows) => {
      filesystems = rows;
    });
    tick();

    expect(filesystems[0].mirroringStatus).toBe('Disabled');
  }));

  it('should produce empty filesystems when list is empty', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(of([]));

    fixture.detectChanges();

    let filesystems: any[] = [];
    component.filesystems$.subscribe((rows) => {
      filesystems = rows;
    });
    tick();

    expect(filesystems).toEqual([]);
    expect(cephfsServiceMock.getCephfs).not.toHaveBeenCalled();
  }));

  it('should skip null details when getCephfs errors', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(of([{ id: 3 }]));
    cephfsServiceMock.getCephfs.mockReturnValue(throwError(() => new Error('boom')));

    fixture.detectChanges();

    let filesystems: any[] = [];
    component.filesystems$.subscribe((rows) => {
      filesystems = rows;
    });
    tick();

    expect(filesystems).toEqual([]);
  }));

  it('should update selection reference', () => {
    const selection = new CdTableSelection([{ id: 1 }]);
    component.updateSelection(selection);
    expect(component.selection).toBe(selection);
  });
});
