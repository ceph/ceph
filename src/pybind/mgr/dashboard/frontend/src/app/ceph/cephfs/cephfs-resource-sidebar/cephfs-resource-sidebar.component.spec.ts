import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, ParamMap, Router } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, jest } from '@jest/globals';

import { CephfsResourceSidebarComponent } from './cephfs-resource-sidebar.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { CephfsActionService } from '~/app/shared/services/cephfs-action.service';
import { CephfsResourceStateService } from '~/app/shared/services/cephfs-resource-state.service';

describe('CephfsResourceSidebarComponent', () => {
  let component: CephfsResourceSidebarComponent;
  let fixture: ComponentFixture<CephfsResourceSidebarComponent>;

  let mockCephfsResourceStateService: { load: jest.Mock; filesystem$: any };
  const mockAuthStorageService = {
    getPermissions: jest.fn(() => ({
      cephfs: { read: true, create: true, update: true, delete: true },
      configOpt: { read: false }
    }))
  };
  const mockRouter = { navigate: jest.fn() };
  const mockActionLabels = {
    EDIT: 'Edit',
    AUTHORIZE: 'Authorize',
    ATTACH: 'Attach',
    REMOVE: 'Remove'
  };
  const mockCephfsActionService = {
    getMonAllowPoolDelete: jest.fn(() => of(false)),
    getDeleteDisableDesc: jest.fn(() => true),
    showAttachInfo: jest.fn(),
    removeVolume: jest.fn(),
    authorize: jest.fn()
  };
  let paramMapSubject: BehaviorSubject<ParamMap>;
  let filesystemSubject: BehaviorSubject<any>;

  beforeEach(async () => {
    paramMapSubject = new BehaviorSubject<ParamMap>(convertToParamMap({ id: 'test-fs-id' }));
    filesystemSubject = new BehaviorSubject<any>(null);

    // Mock the state service
    mockCephfsResourceStateService = {
      load: jest.fn(),
      filesystem$: filesystemSubject.asObservable()
    };

    await TestBed.configureTestingModule({
      declarations: [CephfsResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: paramMapSubject.asObservable()
          }
        },
        { provide: Router, useValue: mockRouter },
        { provide: AuthStorageService, useValue: mockAuthStorageService },
        { provide: ActionLabelsI18n, useValue: mockActionLabels },
        { provide: CephfsActionService, useValue: mockCephfsActionService }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(CephfsResourceSidebarComponent, {
        set: {
          providers: [
            { provide: CephfsResourceStateService, useValue: mockCephfsResourceStateService }
          ]
        }
      })
      .compileComponents();

    fixture = TestBed.createComponent(CephfsResourceSidebarComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Initialization (ngOnInit)', () => {
    it('should read the id param, call load() on state service, and build sidebar items', () => {
      // Act
      fixture.detectChanges(); // Triggers ngOnInit

      // Assert
      expect(component.fsId).toBe('test-fs-id');
      expect(mockCephfsResourceStateService.load).toHaveBeenCalledWith('test-fs-id');

      // Verify sidebar items are built completely
      expect(component.sidebarItems.length).toBe(8);

      const overviewItem = component.sidebarItems[0];
      expect(overviewItem.label).toBe('Overview');
      expect(overviewItem.route).toEqual(['/cephfs/fs', 'test-fs-id', 'overview']);
      expect(overviewItem.routerLinkActiveOptions).toEqual({ exact: true });

      const performanceItem = component.sidebarItems[7];
      expect(performanceItem.label).toBe('Performance');
      expect(performanceItem.route).toEqual(['/cephfs/fs', 'test-fs-id', 'performance']);
    });

    it('should handle a missing id parameter gracefully', () => {
      // Arrange
      paramMapSubject.next(convertToParamMap({}));

      // Act
      fixture.detectChanges();

      // Assert
      expect(component.fsId).toBe('');
      expect(mockCephfsResourceStateService.load).toHaveBeenCalledWith('');
      expect(component.sidebarItems[0].route).toEqual(['/cephfs/fs', '', 'overview']);
    });
  });

  describe('Filesystem Name Resolution', () => {
    beforeEach(() => {
      fixture.detectChanges(); // Trigger init so subscriptions are set up
    });

    it('should set fsName using mdsmap.fs_name if available', () => {
      filesystemSubject.next({
        mdsmap: { fs_name: 'primary-mds-name' },
        cephfs: { name: 'secondary-cephfs-name' }
      });

      expect(component.fsName).toBe('primary-mds-name');
    });

    it('should set fsName using cephfs.name if mdsmap.fs_name is missing', () => {
      filesystemSubject.next({
        mdsmap: {}, // Missing fs_name
        cephfs: { name: 'secondary-cephfs-name' }
      });

      expect(component.fsName).toBe('secondary-cephfs-name');
    });

    it('should fallback to fsId if neither mdsmap.fs_name nor cephfs.name are available', () => {
      filesystemSubject.next(null); // No filesystem data emitted yet
      expect(component.fsName).toBe('test-fs-id');

      filesystemSubject.next({}); // Empty filesystem object
      expect(component.fsName).toBe('test-fs-id');
    });
  });

  describe('Header status and actions', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    it('should set Enabled status and show 4 actions when filesystem is enabled', () => {
      filesystemSubject.next({
        id: 123,
        mdsmap: { fs_name: 'enabled-fs', enabled: true }
      });

      expect(component.headerStatus).toEqual({ type: 'success', text: 'Enabled' });
      expect(component.headerActions.map((action) => action.label)).toEqual([
        'Edit',
        'Authorize',
        'Attach',
        'Remove'
      ]);
    });

    it('should set Disabled status when filesystem is disabled', () => {
      filesystemSubject.next({
        id: 123,
        mdsmap: { fs_name: 'disabled-fs', enabled: false }
      });

      expect(component.headerStatus).toEqual({ type: 'danger', text: 'Disabled' });
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from route and state subscriptions', () => {
      fixture.detectChanges();

      // Access the private subscription object
      const unsubscribeSpy = jest.spyOn((component as any).sub, 'unsubscribe');

      component.ngOnDestroy();

      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });
});
