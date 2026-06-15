import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, fakeAsync, flush, TestBed, tick } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, Router } from '@angular/router';
import { asyncScheduler, defer, of, throwError } from 'rxjs';
import { observeOn } from 'rxjs/operators';

import { CephfsAddMirroringPathComponent } from './cephfs-add-mirroring-path.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';

describe('CephfsAddMirroringPathComponent', () => {
  let component: CephfsAddMirroringPathComponent;
  let fixture: ComponentFixture<CephfsAddMirroringPathComponent>;
  let routerNavigateSpy: jest.Mock;

  const cephfsServiceMock = {
    addMirrorDirectory: jest.fn()
  };

  const snapScheduleServiceMock = {
    create: jest.fn()
  };

  const notificationServiceMock = {
    show: jest.fn()
  };

  const scheduleStepMock = {
    formGroup: {
      markAllAsTouched: jest.fn(),
      updateValueAndValidity: jest.fn(),
      invalid: false
    },
    buildCreatePayload: jest.fn(
      (_fsName: string, selection: { path: string; subvol?: string; group?: string }) => ({
        fs: 'testfs',
        path: selection.path,
        snap_schedule: '1d',
        start: '2023-01-01T00:00:00',
        ...(selection.subvol ? { subvol: selection.subvol } : {}),
        ...(selection.subvol && selection.group ? { group: selection.group } : {})
      })
    )
  };

  const pathsStepMock = {
    getSubmitPaths: jest.fn().mockReturnValue({ toAdd: [], alreadyMirrored: [] }),
    getPathSelections: jest.fn().mockReturnValue([]),
    addTrackedPath: jest.fn()
  };

  function mockTearsheet(
    pathsOverrides: Record<string, unknown> = {},
    scheduleOverrides: Record<string, unknown> = {}
  ): void {
    const pathsStep = { ...pathsStepMock, ...pathsOverrides };
    const scheduleStep = { ...scheduleStepMock, ...scheduleOverrides };
    component.tearsheet = {
      stepContents: {
        first: { stepComponent: pathsStep },
        toArray: () => [{ stepComponent: pathsStep }, { stepComponent: scheduleStep }]
      }
    } as any;
  }

  beforeEach(async () => {
    jest.clearAllMocks();
    routerNavigateSpy = jest.fn();
    pathsStepMock.getSubmitPaths.mockReturnValue({ toAdd: [], alreadyMirrored: [] });
    pathsStepMock.getPathSelections.mockReturnValue([]);
    scheduleStepMock.formGroup.invalid = false;

    await TestBed.configureTestingModule({
      declarations: [CephfsAddMirroringPathComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              paramMap: convertToParamMap({ fsId: '1', fsName: 'testfs' })
            }
          }
        },
        {
          provide: Router,
          useValue: { navigate: routerNavigateSpy }
        },
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: CephfsSnapshotScheduleService, useValue: snapScheduleServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(CephfsAddMirroringPathComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsAddMirroringPathComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read route params on ngOnInit', () => {
    component.ngOnInit();

    expect(component.fsName).toBe('testfs');
    expect(component.fsId).toBe(1);
  });

  it('should define tearsheet metadata', () => {
    expect(component.modalHeaderLabel).toBeDefined();
    expect(component.title).toBeDefined();
    expect(component.steps.length).toBe(3);
    expect(component.steps.map((step) => step.label)).toEqual(['Paths', 'Schedule', 'Review']);
  });

  it('should skip API calls when the schedule step form is invalid', () => {
    component.ngOnInit();
    const scheduleForm = {
      markAllAsTouched: jest.fn(),
      updateValueAndValidity: jest.fn(),
      invalid: true
    };
    mockTearsheet(
      { getSubmitPaths: () => ({ toAdd: ['/volumes/g1/sv1'], alreadyMirrored: [] }) },
      { formGroup: scheduleForm }
    );

    component.onSubmit();

    expect(scheduleForm.markAllAsTouched).toHaveBeenCalled();
    expect(notificationServiceMock.show).not.toHaveBeenCalled();
    expect(cephfsServiceMock.addMirrorDirectory).not.toHaveBeenCalled();
    expect(component.isSubmitLoading).toBe(false);
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  });

  it('should create schedule only for already mirrored paths', fakeAsync(() => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({
        toAdd: [],
        alreadyMirrored: ['/volumes/g1/sv1']
      }),
      getPathSelections: () => [{ path: '/volumes/g1/sv1' }]
    });

    snapScheduleServiceMock.create.mockReturnValue(of({ status: 200 }));

    component.onSubmit();
    tick();
    flush();

    expect(cephfsServiceMock.addMirrorDirectory).not.toHaveBeenCalled();
    expect(snapScheduleServiceMock.create).toHaveBeenCalledTimes(1);
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should add mirror directories and create schedules on success', fakeAsync(() => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'],
        alreadyMirrored: []
      }),
      getPathSelections: () => [{ path: '/volumes/g1/sv1' }, { path: '/volumes/g1/sv2' }]
    });

    cephfsServiceMock.addMirrorDirectory.mockImplementation((_fs: string, path: string) =>
      defer(() => of({ path }).pipe(observeOn(asyncScheduler)))
    );
    snapScheduleServiceMock.create.mockImplementation(() =>
      defer(() => of({ status: 200 }).pipe(observeOn(asyncScheduler)))
    );

    component.onSubmit();
    tick();
    flush();

    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledTimes(2);
    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledWith('testfs', '/volumes/g1/sv1');
    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledWith('testfs', '/volumes/g1/sv2');
    expect(snapScheduleServiceMock.create).toHaveBeenCalledTimes(2);
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should show error notifications when mirror directory add fails', fakeAsync(() => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'],
        alreadyMirrored: []
      }),
      getPathSelections: () => [{ path: '/volumes/g1/sv1' }, { path: '/volumes/g1/sv2' }]
    });

    cephfsServiceMock.addMirrorDirectory.mockImplementation((_fs: string, path: string) =>
      throwError(() => ({
        error: { detail: `failed for ${path}` }
      }))
    );

    component.onSubmit();
    tick();
    flush();

    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledTimes(2);
    expect(
      notificationServiceMock.show.mock.calls.filter(([type]) => type === NotificationType.error)
        .length
    ).toBe(2);
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  }));

  it('should handle partial failures with success and error notifications', fakeAsync(() => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'],
        alreadyMirrored: []
      }),
      getPathSelections: () => [{ path: '/volumes/g1/sv1' }, { path: '/volumes/g1/sv2' }]
    });

    cephfsServiceMock.addMirrorDirectory.mockImplementation((_fs: string, path: string) =>
      path === '/volumes/g1/sv1'
        ? of({ path })
        : throwError(() => ({ error: { detail: `failed for ${path}` } }))
    );
    snapScheduleServiceMock.create.mockReturnValue(of({ status: 200 }));

    component.onSubmit();
    tick();
    flush();

    expect(
      notificationServiceMock.show.mock.calls.filter(([type]) => type === NotificationType.success)
        .length
    ).toBeGreaterThanOrEqual(1);
    expect(
      notificationServiceMock.show.mock.calls.filter(([type]) => type === NotificationType.error)
        .length
    ).toBeGreaterThanOrEqual(1);
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should show error when no paths are selected on submit', () => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({ toAdd: [], alreadyMirrored: [] }),
      getPathSelections: () => []
    });

    component.onSubmit();

    expect(notificationServiceMock.show).toHaveBeenCalledWith(
      NotificationType.error,
      expect.stringContaining('Select at least one path')
    );
    expect(cephfsServiceMock.addMirrorDirectory).not.toHaveBeenCalled();
    expect(component.isSubmitLoading).toBe(false);
  });

  it('should close tearsheet without reload on cancel', () => {
    component.ngOnInit();
    component.onClose();

    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: undefined }
    );
  });

  it('should cache paths step state on step change', () => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1'],
        alreadyMirrored: ['/volumes/g1/sv2']
      }),
      getPathSelections: () => [
        { path: '/volumes/g1/sv1', subvol: 'sv1', group: 'g1' },
        { path: '/volumes/g1/sv2', subvol: 'sv2', group: 'g1' }
      ]
    });

    component.onStepChanged();

    expect(component.steps[0].invalid).toBe(false);
  });

  it('should mark paths step invalid when no paths are selected', () => {
    component.ngOnInit();
    mockTearsheet({
      getSubmitPaths: () => ({ toAdd: [], alreadyMirrored: [] }),
      getPathSelections: () => []
    });

    component.onStepChanged();

    expect(component.steps[0].invalid).toBe(true);
  });

  it('should decode encoded filesystem name from route params', () => {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
      declarations: [CephfsAddMirroringPathComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              paramMap: convertToParamMap({ fsId: '2', fsName: encodeURIComponent('my fs') })
            }
          }
        },
        {
          provide: Router,
          useValue: { navigate: routerNavigateSpy }
        },
        { provide: CephfsService, useValue: cephfsServiceMock },
        { provide: CephfsSnapshotScheduleService, useValue: snapScheduleServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(CephfsAddMirroringPathComponent, {
        set: { template: '' }
      })
      .compileComponents();

    const decodedFixture = TestBed.createComponent(CephfsAddMirroringPathComponent);
    decodedFixture.componentInstance.ngOnInit();

    expect(decodedFixture.componentInstance.fsName).toBe('my fs');
    expect(decodedFixture.componentInstance.fsId).toBe(2);
  });
});
