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
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

describe('CephfsAddMirroringPathComponent', () => {
  let component: CephfsAddMirroringPathComponent;
  let fixture: ComponentFixture<CephfsAddMirroringPathComponent>;
  let routerNavigateSpy: jest.Mock;

  const cephfsServiceMock = {
    addMirrorDirectory: jest.fn()
  };

  const snapshotScheduleServiceMock = {
    create: jest.fn()
  };

  const taskWrapperMock = {
    wrapTaskAroundCall: jest.fn(({ call }) => call)
  };

  const notificationServiceMock = {
    show: jest.fn()
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    routerNavigateSpy = jest.fn();

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
        { provide: CephfsSnapshotScheduleService, useValue: snapshotScheduleServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperMock },
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

  it('should skip API calls when the paths step form is invalid', () => {
    const refreshTrackedPaths = jest.fn(() => of(undefined));
    component.pathsStep = {
      formGroup: {
        markAllAsTouched: jest.fn(),
        updateValueAndValidity: jest.fn(),
        invalid: true
      },
      refreshTrackedPaths,
      getSubmitPaths: () => ({ toAdd: [], alreadyMirrored: [] })
    } as any;

    component.onSubmit();

    expect(refreshTrackedPaths).toHaveBeenCalled();
    expect(notificationServiceMock.show).not.toHaveBeenCalled();
    expect(cephfsServiceMock.addMirrorDirectory).not.toHaveBeenCalled();
    expect(component.isSubmitLoading).toBe(false);
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  });

  function mockValidPathsStep(overrides: Record<string, unknown> = {}): void {
    component.pathsStep = {
      formGroup: {
        markAllAsTouched: jest.fn(),
        updateValueAndValidity: jest.fn(),
        invalid: false
      },
      refreshTrackedPaths: () => of(undefined),
      getSubmitPaths: () => ({ toAdd: [], alreadyMirrored: [] }),
      getSelectedPaths: () => [],
      addTrackedPath: jest.fn(),
      ...overrides
    } as any;
    component.scheduleStep = {
      buildCreatePayload: jest.fn((path: string) => ({ path, fs: 'testfs' }))
    } as any;
    snapshotScheduleServiceMock.create.mockReturnValue(of({}));
  }

  it('should add mirror directories and close modal on success', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({ toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'], alreadyMirrored: [] }),
      getSelectedPaths: () => ['/volumes/g1/sv1', '/volumes/g1/sv2']
    });

    cephfsServiceMock.addMirrorDirectory.mockImplementation((_fs: string, path: string) =>
      defer(() => of({ path }).pipe(observeOn(asyncScheduler)))
    );

    component.onSubmit();
    tick();
    flush();

    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledTimes(2);
    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledWith('testfs', '/volumes/g1/sv1');
    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledWith('testfs', '/volumes/g1/sv2');
    expect(component.scheduleStep.buildCreatePayload).toHaveBeenCalledTimes(2);
    expect(component.scheduleStep.buildCreatePayload).toHaveBeenCalledWith('/volumes/g1/sv1');
    expect(component.scheduleStep.buildCreatePayload).toHaveBeenCalledWith('/volumes/g1/sv2');
    expect(snapshotScheduleServiceMock.create).toHaveBeenCalledTimes(2);
    expect(
      notificationServiceMock.show.mock.calls.filter(([type]) => type === NotificationType.success)
        .length
    ).toBe(1);
    expect(notificationServiceMock.show).toHaveBeenCalledWith(
      NotificationType.success,
      expect.stringContaining('2'),
      expect.stringMatching(/\/volumes\/g1\/sv1[\s\S]*\/volumes\/g1\/sv2/)
    );
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should show a single aggregated error notification when paths fail', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'],
        alreadyMirrored: []
      }),
      getSelectedPaths: () => ['/volumes/g1/sv1', '/volumes/g1/sv2']
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
    ).toBe(1);
    expect(notificationServiceMock.show).toHaveBeenCalledWith(
      NotificationType.error,
      expect.stringContaining('2'),
      expect.stringContaining('/volumes/g1/sv1')
    );
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  }));

  it('should show aggregated success and error notifications for partial failures', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1', '/volumes/g1/sv2'],
        alreadyMirrored: []
      }),
      getSelectedPaths: () => ['/volumes/g1/sv1', '/volumes/g1/sv2']
    });

    cephfsServiceMock.addMirrorDirectory.mockImplementation((_fs: string, path: string) =>
      path === '/volumes/g1/sv1'
        ? of({ path })
        : throwError(() => ({ error: { detail: `failed for ${path}` } }))
    );

    component.onSubmit();
    tick();
    flush();

    expect(
      notificationServiceMock.show.mock.calls.filter(([type]) => type === NotificationType.success)
        .length
    ).toBe(1);
    expect(
      notificationServiceMock.show.mock.calls.filter(([type]) => type === NotificationType.error)
        .length
    ).toBe(1);
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should show warning instead of success when server reports path already tracked', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({
        toAdd: ['/volumes/g1/sv1'],
        alreadyMirrored: []
      }),
      getSelectedPaths: () => ['/volumes/g1/sv1']
    });

    cephfsServiceMock.addMirrorDirectory.mockImplementation((_fs: string, path: string) =>
      throwError({ error: { detail: `directory ${path} is already tracked` } })
    );

    component.onSubmit();
    tick();
    flush();

    expect(cephfsServiceMock.addMirrorDirectory).toHaveBeenCalledWith('testfs', '/volumes/g1/sv1');
    expect(
      notificationServiceMock.show.mock.calls.some(
        ([type, message]) =>
          type === NotificationType.success && message.includes('Mirroring path')
      )
    ).toBe(false);
    expect(
      notificationServiceMock.show.mock.calls.some(
        ([type, message]) =>
          type === NotificationType.warning && message.includes('Skipped 1 path')
      )
    ).toBe(true);
    expect(snapshotScheduleServiceMock.create).toHaveBeenCalledWith({
      path: '/volumes/g1/sv1',
      fs: 'testfs'
    });
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should create snapshot schedules for already mirrored paths without adding mirror paths', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({
        toAdd: [],
        alreadyMirrored: ['/volumes/g1/sv1']
      }),
      getSelectedPaths: () => ['/volumes/g1/sv1']
    });

    component.onSubmit();
    tick();
    flush();

    expect(cephfsServiceMock.addMirrorDirectory).not.toHaveBeenCalled();
    expect(snapshotScheduleServiceMock.create).toHaveBeenCalledTimes(1);
    expect(snapshotScheduleServiceMock.create).toHaveBeenCalledWith({
      path: '/volumes/g1/sv1',
      fs: 'testfs'
    });
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

  it('should create snapshot schedules for multiple already mirrored paths', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({
        toAdd: [],
        alreadyMirrored: ['/volumes/g1/sv1', '/volumes/g1/sv2']
      }),
      getSelectedPaths: () => ['/volumes/g1/sv1', '/volumes/g1/sv2']
    });

    component.onSubmit();
    tick();
    flush();

    expect(snapshotScheduleServiceMock.create).toHaveBeenCalledTimes(2);
    expect(component.scheduleStep.buildCreatePayload).toHaveBeenCalledWith('/volumes/g1/sv1');
    expect(component.scheduleStep.buildCreatePayload).toHaveBeenCalledWith('/volumes/g1/sv2');
  }));

  it('should close modal outlet on cancel without reload', () => {
    component.ngOnInit();
    component.onCancel();

    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: undefined }
    );
  });

  it('should create snapshot schedules through TaskWrapper after paths succeed', fakeAsync(() => {
    component.ngOnInit();
    mockValidPathsStep({
      getSubmitPaths: () => ({ toAdd: ['/volumes/g1/sv1'], alreadyMirrored: [] }),
      getSelectedPaths: () => ['/volumes/g1/sv1']
    });
    component.scheduleStep = {
      buildCreatePayload: jest.fn((path: string) => ({ path, fs: 'testfs' }))
    } as any;

    cephfsServiceMock.addMirrorDirectory.mockReturnValue(of({ path: '/volumes/g1/sv1' }));
    snapshotScheduleServiceMock.create.mockReturnValue(of({}));

    component.onSubmit();
    tick();
    flush();

    expect(taskWrapperMock.wrapTaskAroundCall).toHaveBeenCalledWith({
      task: expect.objectContaining({
        name: 'cephfs/snapshot/schedule/create',
        metadata: { path: '/volumes/g1/sv1' }
      }),
      call: expect.anything()
    });
    expect(snapshotScheduleServiceMock.create).toHaveBeenCalledWith({
      path: '/volumes/g1/sv1',
      fs: 'testfs'
    });
    expect(routerNavigateSpy).toHaveBeenCalledWith(
      ['/cephfs/mirroring', { outlets: { modal: null } }],
      { state: { reload: true } }
    );
  }));

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
        { provide: CephfsSnapshotScheduleService, useValue: snapshotScheduleServiceMock },
        { provide: TaskWrapperService, useValue: taskWrapperMock },
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
