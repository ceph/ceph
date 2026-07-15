import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of } from 'rxjs';

import { MirroringPathsStepComponent } from './mirroring-paths-step.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { createPathEntry } from '../mirroring-path.model';
import { FS_ROOT_PATH_SENTINEL } from '../mirroring-path-utils';

describe('MirroringPathsStepComponent', () => {
  let component: MirroringPathsStepComponent;
  let fixture: ComponentFixture<MirroringPathsStepComponent>;

  const cephfsServiceMock = {
    list: jest.fn().mockReturnValue(of([])),
    lsDir: jest.fn().mockReturnValue(of([])),
    listMirrorDirectories: jest.fn().mockReturnValue(of([]))
  };

  function mockLsDirTree(): void {
    cephfsServiceMock.lsDir.mockImplementation((_id: number, path: string) => {
      if (path === '/') {
        return of([
          { name: 'mirror', parent: '/' },
          { name: 'volumes', parent: '/' }
        ]);
      }
      if (path === '/volumes') {
        return of([{ name: 'g1', parent: '/volumes' }]);
      }
      if (path === '/volumes/g1') {
        return of([
          { name: 'sv1', parent: '/volumes/g1' },
          { name: 'sv2', parent: '/volumes/g1' }
        ]);
      }
      return of([]);
    });
  }

  function selectVolumesG1Sv1(): void {
    component.onLevelChange(0, 0, 'volumes');
    tick();
    component.onLevelChange(0, 1, 'g1');
    tick();
    component.onLevelChange(0, 2, 'sv1');
  }

  function selectVolumesG1Sv2(): void {
    component.onLevelChange(0, 0, 'volumes');
    tick();
    component.onLevelChange(0, 1, 'g1');
    tick();
    component.onLevelChange(0, 2, 'sv2');
  }

  beforeEach(async () => {
    jest.clearAllMocks();

    await TestBed.configureTestingModule({
      declarations: [MirroringPathsStepComponent],
      imports: [ReactiveFormsModule],
      providers: [{ provide: CephfsService, useValue: cephfsServiceMock }],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(MirroringPathsStepComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(MirroringPathsStepComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form group with required pathsControl', () => {
    component.ngOnInit();

    expect(component.formGroup).toBeTruthy();
    expect(component.pathsControl.hasError('required')).toBe(true);
    expect(component.paths.length).toBe(1);
  });

  it('should expose inline validation when no path is selected', () => {
    component.ngOnInit();
    component.pathsControl.markAsTouched();

    expect(component.pathsError).toContain('Select at least one path');
  });

  it('should hide already mirrored paths from dropdown options', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, 'volumes');
    tick();
    component.onLevelChange(0, 1, 'g1');
    tick();

    expect(component.paths[0].levels[2].options).toEqual(['sv2']);
  }));

  it('should expose inline validation when only already mirrored paths are selected', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    selectVolumesG1Sv1();
    component.pathsControl.markAsTouched();

    expect(component.pathsControl.hasError('alreadyMirrored')).toBe(true);
    expect(component.pathsError).toContain('already mirrored');
  }));

  it('should not load initial data when fsName is missing', () => {
    component.ngOnInit();

    expect(cephfsServiceMock.lsDir).not.toHaveBeenCalled();
    expect(cephfsServiceMock.listMirrorDirectories).not.toHaveBeenCalled();
  });

  it('should load filesystem root options and tracked paths on init', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of([]));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    expect(cephfsServiceMock.lsDir).toHaveBeenCalledWith(1, '/', 1);
    expect(cephfsServiceMock.listMirrorDirectories).toHaveBeenCalledWith('testfs');
    expect(component.paths[0].levels[0].options).toEqual([
      FS_ROOT_PATH_SENTINEL,
      'mirror',
      'volumes'
    ]);
  }));

  it('should resolve fsId from cephfsService when fsId input is not set', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.list.mockReturnValue(of([{ id: 5, mdsmap: { fs_name: 'testfs' } }]));

    component.fsName = 'testfs';
    component.ngOnInit();
    tick();

    expect(cephfsServiceMock.list).toHaveBeenCalled();
    expect(component.fsId).toBe(5);
    expect(cephfsServiceMock.lsDir).toHaveBeenCalledWith(5, '/', 1);
  }));

  it('should add and remove path entries', fakeAsync(() => {
    mockLsDirTree();
    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.addPath();
    tick();
    expect(component.paths.length).toBe(2);

    component.removePath(1);
    expect(component.paths.length).toBe(1);
  }));

  it('should toggle path expansion', () => {
    component.paths = [createPathEntry(true)];
    expect(component.paths[0].expanded).toBe(true);

    component.toggleExpand(0);
    expect(component.paths[0].expanded).toBe(false);
  });

  it('should classify submit paths as toAdd or alreadyMirrored', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    selectVolumesG1Sv2();

    expect(component.getSubmitPaths()).toEqual({
      toAdd: ['/volumes/g1/sv2'],
      alreadyMirrored: []
    });
  }));

  it('should refresh tracked paths from the server', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of([]));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    selectVolumesG1Sv1();
    expect(component.getSubmitPaths().toAdd).toEqual(['/volumes/g1/sv1']);

    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    let completed = false;
    component.refreshTrackedPaths().subscribe(() => {
      completed = true;
    });
    tick();

    expect(completed).toBe(true);
    expect(component.getSubmitPaths().alreadyMirrored).toEqual(['/volumes/g1/sv1']);
  }));

  it('should allow sibling subvolume paths across multiple rows', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of([]));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    selectVolumesG1Sv1();
    tick();

    component.addPath();
    tick();

    expect(component.paths[1].levels[0].options).toEqual([
      FS_ROOT_PATH_SENTINEL,
      'mirror',
      'volumes'
    ]);

    component.onLevelChange(1, 0, 'volumes');
    tick();
    component.onLevelChange(1, 1, 'g1');
    tick();
    expect(component.paths[1].levels[2].options).toEqual(['sv2']);
    component.onLevelChange(1, 2, 'sv2');

    expect(component.getSubmitPaths()).toEqual({
      toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'],
      alreadyMirrored: []
    });
  }));

  it('should allow selecting top-level directories such as /mirror', fakeAsync(() => {
    mockLsDirTree();
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of([]));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, 'mirror');
    expect(component.getSubmitPaths().toAdd).toEqual(['/mirror']);
  }));

  it('should show a warning when the filesystem root path is selected', fakeAsync(() => {
    mockLsDirTree();
    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, FS_ROOT_PATH_SENTINEL);

    expect(component.showRootWarning).toBe(true);
    expect(component.canAddAnotherPath).toBe(false);
  }));

  it('should add tracked path locally after successful submit', fakeAsync(() => {
    mockLsDirTree();

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    selectVolumesG1Sv2();
    expect(component.getSubmitPaths().toAdd).toEqual(['/volumes/g1/sv2']);

    component.addTrackedPath('/volumes/g1/sv2');
    expect(component.getSubmitPaths()).toEqual({
      toAdd: [],
      alreadyMirrored: ['/volumes/g1/sv2']
    });
  }));
});
