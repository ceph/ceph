import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { of } from 'rxjs';

import { MirroringPathsStepComponent } from './mirroring-paths-step.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';
import { createPathEntry } from '../mirroring-path.model';

describe('MirroringPathsStepComponent', () => {
  let component: MirroringPathsStepComponent;
  let fixture: ComponentFixture<MirroringPathsStepComponent>;

  const cephfsServiceMock = {
    list: jest.fn().mockReturnValue(of([])),
    lsDir: jest.fn().mockReturnValue(of([])),
    listMirrorDirectories: jest.fn().mockReturnValue(of([]))
  };

  const subvolumeGroupServiceMock = {
    get: jest.fn().mockReturnValue(of([]))
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    await TestBed.configureTestingModule({
      declarations: [MirroringPathsStepComponent],
      imports: [ReactiveFormsModule],
      providers: [
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: CephfsSubvolumeGroupService, useValue: subvolumeGroupServiceMock }
      ],
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

  it('should expose inline validation when only already mirrored paths are selected', fakeAsync(() => {
    subvolumeGroupServiceMock.get.mockReturnValue(of([{ name: 'g1' }]));
    cephfsServiceMock.lsDir.mockReturnValue(
      of([
        { name: 'g1', parent: '/volumes' },
        { name: 'sv1', parent: '/volumes/g1' }
      ])
    );
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, 'g1');
    component.onLevelChange(0, 1, 'sv1');
    component.pathsControl.markAsTouched();

    expect(component.pathsControl.hasError('alreadyMirrored')).toBe(true);
    expect(component.pathsError).toContain('already mirrored');
  }));

  it('should not load initial data when fsName is missing', () => {
    component.ngOnInit();

    expect(subvolumeGroupServiceMock.get).not.toHaveBeenCalled();
    expect(cephfsServiceMock.lsDir).not.toHaveBeenCalled();
    expect(cephfsServiceMock.listMirrorDirectories).not.toHaveBeenCalled();
  });

  it('should load groups, directory tree, and tracked paths on init', fakeAsync(() => {
    subvolumeGroupServiceMock.get.mockReturnValue(of([{ name: 'g1' }, { name: 'g2' }]));
    cephfsServiceMock.lsDir.mockReturnValue(
      of([
        { name: 'g1', parent: '/volumes' },
        { name: 'sv1', parent: '/volumes/g1' }
      ])
    );
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    expect(subvolumeGroupServiceMock.get).toHaveBeenCalledWith('testfs', false);
    expect(cephfsServiceMock.lsDir).toHaveBeenCalledWith(1, '/volumes', 3);
    expect(cephfsServiceMock.listMirrorDirectories).toHaveBeenCalledWith('testfs');
    expect(component.paths[0].levels[0].options).toEqual([DEFAULT_SUBVOLUME_GROUP, 'g1', 'g2']);
  }));

  it('should resolve fsId from cephfsService when fsId input is not set', fakeAsync(() => {
    cephfsServiceMock.list.mockReturnValue(of([{ id: 5, mdsmap: { fs_name: 'testfs' } }]));

    component.fsName = 'testfs';
    component.ngOnInit();
    tick();

    expect(cephfsServiceMock.list).toHaveBeenCalled();
    expect(component.fsId).toBe(5);
    expect(cephfsServiceMock.lsDir).toHaveBeenCalledWith(5, '/volumes', 3);
  }));

  it('should add and remove path entries', fakeAsync(() => {
    subvolumeGroupServiceMock.get.mockReturnValue(of([{ name: 'g1' }]));
    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.addPath();
    expect(component.paths.length).toBe(2);

    component.removePath(1);
    expect(component.paths.length).toBe(1);
  }));

  it('should toggle path expansion', () => {
    component.paths = [createPathEntry([], true)];
    expect(component.paths[0].expanded).toBe(true);

    component.toggleExpand(0);
    expect(component.paths[0].expanded).toBe(false);
  });

  it('should classify submit paths as toAdd or alreadyMirrored', fakeAsync(() => {
    subvolumeGroupServiceMock.get.mockReturnValue(of([{ name: 'g1' }]));
    cephfsServiceMock.lsDir.mockReturnValue(
      of([
        { name: 'g1', parent: '/volumes' },
        { name: 'sv1', parent: '/volumes/g1' },
        { name: 'sv2', parent: '/volumes/g1' }
      ])
    );
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, 'g1');
    component.onLevelChange(0, 1, 'sv1');

    expect(component.getSubmitPaths()).toEqual({
      toAdd: [],
      alreadyMirrored: ['/volumes/g1/sv1']
    });

    component.onLevelChange(0, 1, 'sv2');
    expect(component.getSubmitPaths()).toEqual({
      toAdd: ['/volumes/g1/sv2'],
      alreadyMirrored: []
    });
  }));

  it('should refresh tracked paths from the server', fakeAsync(() => {
    subvolumeGroupServiceMock.get.mockReturnValue(of([{ name: 'g1' }]));
    cephfsServiceMock.lsDir.mockReturnValue(
      of([
        { name: 'g1', parent: '/volumes' },
        { name: 'sv1', parent: '/volumes/g1' }
      ])
    );
    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of([]));

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, 'g1');
    component.onLevelChange(0, 1, 'sv1');
    expect(component.getSubmitPaths().alreadyMirrored).toEqual([]);

    cephfsServiceMock.listMirrorDirectories.mockReturnValue(of(['/volumes/g1/sv1']));

    let completed = false;
    component.refreshTrackedPaths().subscribe(() => {
      completed = true;
    });
    tick();

    expect(completed).toBe(true);
    expect(component.getSubmitPaths().alreadyMirrored).toEqual(['/volumes/g1/sv1']);
  }));

  it('should add tracked path locally after successful submit', fakeAsync(() => {
    subvolumeGroupServiceMock.get.mockReturnValue(of([{ name: 'g1' }]));
    cephfsServiceMock.lsDir.mockReturnValue(
      of([
        { name: 'g1', parent: '/volumes' },
        { name: 'sv2', parent: '/volumes/g1' }
      ])
    );

    component.fsName = 'testfs';
    component.fsId = 1;
    component.ngOnInit();
    tick();

    component.onLevelChange(0, 0, 'g1');
    component.onLevelChange(0, 1, 'sv2');
    expect(component.getSubmitPaths().toAdd).toEqual(['/volumes/g1/sv2']);

    component.addTrackedPath('/volumes/g1/sv2');
    expect(component.getSubmitPaths()).toEqual({
      toAdd: [],
      alreadyMirrored: ['/volumes/g1/sv2']
    });
  }));
});
