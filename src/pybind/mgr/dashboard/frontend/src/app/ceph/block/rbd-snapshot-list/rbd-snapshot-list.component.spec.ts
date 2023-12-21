import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbModalModule, NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { MockComponent } from 'ng-mocks';
import { ToastrModule } from 'ngx-toastr';
import { Subject, throwError as observableThrowError } from 'rxjs';

import { RbdService } from '~/app/shared/api/rbd.service';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { Permissions } from '~/app/shared/models/permissions';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { configureTestBed, expectItemTasks, PermissionHelper } from '~/testing/unit-test-helper';
import { RbdSnapshotFormModalComponent } from '../rbd-snapshot-form/rbd-snapshot-form-modal.component';
import { RbdTabsComponent } from '../rbd-tabs/rbd-tabs.component';
import { RbdSnapshotActionsModel } from './rbd-snapshot-actions.model';
import { RbdSnapshotListComponent } from './rbd-snapshot-list.component';
import { RbdSnapshotModel } from './rbd-snapshot.model';

describe('RbdSnapshotListComponent', () => {
  let component: RbdSnapshotListComponent;
  let fixture: ComponentFixture<RbdSnapshotListComponent>;
  let summaryService: SummaryService;

  const fakeAuthStorageService = {
    isLoggedIn: () => {
      return true;
    },
    getPermissions: () => {
      return new Permissions({ 'rbd-image': ['read', 'update', 'create', 'delete'] });
    }
  };

  configureTestBed(
    {
      declarations: [
        RbdSnapshotListComponent,
        RbdTabsComponent,
        MockComponent(RbdSnapshotFormModalComponent)
      ],
      imports: [
        BrowserAnimationsModule,
        ComponentsModule,
        DataTableModule,
        HttpClientTestingModule,
        PipesModule,
        RouterTestingModule,
        NgbNavModule,
        ToastrModule.forRoot(),
        NgbModalModule
      ],
      providers: [
        { provide: AuthStorageService, useValue: fakeAuthStorageService },
        TaskListService
      ]
    },
    [CriticalConfirmationModalComponent]
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotListComponent);
    component = fixture.componentInstance;
    component.ngOnChanges();
    summaryService = TestBed.inject(SummaryService);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('api delete request', () => {
    let called: boolean;
    let rbdService: RbdService;
    let notificationService: NotificationService;
    let authStorageService: AuthStorageService;

    beforeEach(() => {
      fixture.detectChanges();
      const modalService = TestBed.inject(ModalService);
      const actionLabelsI18n = TestBed.inject(ActionLabelsI18n);
      called = false;
      rbdService = new RbdService(null, null);
      notificationService = new NotificationService(null, null, null);
      authStorageService = new AuthStorageService();
      authStorageService.set('user', { 'rbd-image': ['create', 'read', 'update', 'delete'] });
      component = new RbdSnapshotListComponent(
        authStorageService,
        modalService,
        null,
        null,
        rbdService,
        null,
        notificationService,
        null,
        null,
        actionLabelsI18n,
        null
      );
      spyOn(rbdService, 'deleteSnapshot').and.returnValue(observableThrowError({ status: 500 }));
      spyOn(notificationService, 'notifyTask').and.stub();
    });

    it('should call stopLoadingSpinner if the request fails', fakeAsync(() => {
      component.updateSelection(new CdTableSelection([{ name: 'someName' }]));
      expect(called).toBe(false);
      component.deleteSnapshotModal();
      spyOn(component.modalRef.componentInstance, 'stopLoadingSpinner').and.callFake(() => {
        called = true;
      });
      component.modalRef.componentInstance.submitAction();
      tick(500);
      expect(called).toBe(true);
    }));
  });

  describe('handling of executing tasks', () => {
    let snapshots: RbdSnapshotModel[];

    const addSnapshot = (name: string) => {
      const model = new RbdSnapshotModel();
      model.id = 1;
      model.name = name;
      snapshots.push(model);
    };

    const addTask = (task_name: string, snapshot_name: string) => {
      const task = new ExecutingTask();
      task.name = task_name;
      task.metadata = {
        image_spec: 'rbd/foo',
        snapshot_name: snapshot_name
      };
      summaryService.addRunningTask(task);
    };

    const refresh = (data: any) => {
      summaryService['summaryDataSource'].next(data);
    };

    beforeEach(() => {
      fixture.detectChanges();
      snapshots = [];
      addSnapshot('a');
      addSnapshot('b');
      addSnapshot('c');
      component.snapshots = snapshots;
      component.poolName = 'rbd';
      component.rbdName = 'foo';
      refresh({ executing_tasks: [], finished_tasks: [] });
      component.ngOnChanges();
      fixture.detectChanges();
    });

    it('should gets all snapshots without tasks', () => {
      expect(component.snapshots.length).toBe(3);
      expect(component.snapshots.every((image) => !image.cdExecuting)).toBeTruthy();
    });

    it('should add a new image from a task', () => {
      addTask('rbd/snap/create', 'd');
      expect(component.snapshots.length).toBe(4);
      expectItemTasks(component.snapshots[0], undefined);
      expectItemTasks(component.snapshots[1], undefined);
      expectItemTasks(component.snapshots[2], undefined);
      expectItemTasks(component.snapshots[3], 'Creating');
    });

    it('should show when an existing image is being modified', () => {
      addTask('rbd/snap/edit', 'a');
      addTask('rbd/snap/delete', 'b');
      addTask('rbd/snap/rollback', 'c');
      expect(component.snapshots.length).toBe(3);
      expectItemTasks(component.snapshots[0], 'Updating');
      expectItemTasks(component.snapshots[1], 'Deleting');
      expectItemTasks(component.snapshots[2], 'Rolling back');
    });
  });

  describe('snapshot modal dialog', () => {
    beforeEach(() => {
      component.poolName = 'pool01';
      component.rbdName = 'image01';
      spyOn(TestBed.inject(ModalService), 'show').and.callFake(() => {
        const ref: any = {};
        ref.componentInstance = new RbdSnapshotFormModalComponent(
          null,
          null,
          null,
          null,
          TestBed.inject(ActionLabelsI18n),
          null
        );
        ref.componentInstance.onSubmit = new Subject();
        return ref;
      });
    });

    it('should display old snapshot name', () => {
      component.selection.selected = [{ name: 'oldname' }];
      component.openEditSnapshotModal();
      expect(component.modalRef.componentInstance.snapName).toBe('oldname');
      expect(component.modalRef.componentInstance.editing).toBeTruthy();
    });

    it('should display suggested snapshot name', () => {
      component.openCreateSnapshotModal();
      expect(component.modalRef.componentInstance.snapName).toMatch(
        RegExp(`^${component.rbdName}_[\\d-]+T[\\d.:]+[\\+-][\\d:]+$`)
      );
    });
  });

  it('should test all TableActions combinations', () => {
    component.ngOnInit();
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: [
          'Create',
          'Rename',
          'Protect',
          'Unprotect',
          'Clone',
          'Copy',
          'Rollback',
          'Delete'
        ],
        primary: { multiple: 'Create', executing: 'Rename', single: 'Rename', no: 'Create' }
      },
      'create,update': {
        actions: ['Create', 'Rename', 'Protect', 'Unprotect', 'Clone', 'Copy', 'Rollback'],
        primary: { multiple: 'Create', executing: 'Rename', single: 'Rename', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Clone', 'Copy', 'Delete'],
        primary: { multiple: 'Create', executing: 'Clone', single: 'Clone', no: 'Create' }
      },
      create: {
        actions: ['Create', 'Clone', 'Copy'],
        primary: { multiple: 'Create', executing: 'Clone', single: 'Clone', no: 'Create' }
      },
      'update,delete': {
        actions: ['Rename', 'Protect', 'Unprotect', 'Rollback', 'Delete'],
        primary: { multiple: 'Rename', executing: 'Rename', single: 'Rename', no: 'Rename' }
      },
      update: {
        actions: ['Rename', 'Protect', 'Unprotect', 'Rollback'],
        primary: { multiple: 'Rename', executing: 'Rename', single: 'Rename', no: 'Rename' }
      },
      delete: {
        actions: ['Delete'],
        primary: { multiple: 'Delete', executing: 'Delete', single: 'Delete', no: 'Delete' }
      },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });

  describe('clone button disable state', () => {
    let actions: RbdSnapshotActionsModel;

    beforeEach(() => {
      fixture.detectChanges();
      const rbdService = TestBed.inject(RbdService);
      const actionLabelsI18n = TestBed.inject(ActionLabelsI18n);
      actions = new RbdSnapshotActionsModel(actionLabelsI18n, [], rbdService);
    });

    it('should be disabled with version 1 and protected false', () => {
      const selection = new CdTableSelection([{ name: 'someName', is_protected: false }]);
      const disableDesc = actions.getCloneDisableDesc(selection);
      expect(disableDesc).toBe('Snapshot must be protected in order to clone.');
    });

    it.each([
      [1, true],
      [2, true],
      [2, false]
    ])('should be enabled with version %d and protected %s', (version, is_protected) => {
      actions.cloneFormatVersion = version;
      const selection = new CdTableSelection([{ name: 'someName', is_protected: is_protected }]);
      const disableDesc = actions.getCloneDisableDesc(selection);
      expect(disableDesc).toBe(false);
    });
  });

  describe('protect button disable state', () => {
    let actions: RbdSnapshotActionsModel;

    beforeEach(() => {
      fixture.detectChanges();
      const rbdService = TestBed.inject(RbdService);
      const actionLabelsI18n = TestBed.inject(ActionLabelsI18n);
      actions = new RbdSnapshotActionsModel(actionLabelsI18n, [], rbdService);
    });

    it('should be disabled if layering not supported', () => {
      const selection = new CdTableSelection([{ name: 'someName', is_protected: false }]);
      const disableDesc = actions.getProtectDisableDesc(selection, ['deep-flatten', 'fast-diff']);
      expect(disableDesc).toBe('The layering feature needs to be enabled on parent image');
    });
  });
});
