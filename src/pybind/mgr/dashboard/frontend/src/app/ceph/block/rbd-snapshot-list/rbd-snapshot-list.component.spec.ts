import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { Subject, throwError as observableThrowError } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { ApiModule } from '../../../shared/api/api.module';
import { RbdService } from '../../../shared/api/rbd.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { DataTableModule } from '../../../shared/datatable/datatable.module';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { Permissions } from '../../../shared/models/permissions';
import { PipesModule } from '../../../shared/pipes/pipes.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { ServicesModule } from '../../../shared/services/services.module';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
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

  configureTestBed({
    declarations: [RbdSnapshotListComponent],
    imports: [
      DataTableModule,
      ComponentsModule,
      ToastModule.forRoot(),
      ServicesModule,
      ApiModule,
      HttpClientTestingModule,
      RouterTestingModule,
      PipesModule
    ],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      TaskListService,
      i18nProviders
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.get(SummaryService);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('api delete request', () => {
    let called;
    let rbdService: RbdService;
    let notificationService: NotificationService;
    let authStorageService: AuthStorageService;

    beforeEach(() => {
      fixture.detectChanges();
      const i18n = TestBed.get(I18n);
      called = false;
      rbdService = new RbdService(null, null);
      notificationService = new NotificationService(null, null, null);
      authStorageService = new AuthStorageService();
      authStorageService.set('user', '', { 'rbd-image': ['create', 'read', 'update', 'delete'] });
      component = new RbdSnapshotListComponent(
        authStorageService,
        null,
        null,
        null,
        rbdService,
        null,
        notificationService,
        null,
        null,
        i18n
      );
      spyOn(rbdService, 'deleteSnapshot').and.returnValue(observableThrowError({ status: 500 }));
      spyOn(notificationService, 'notifyTask').and.stub();
      component.modalRef = new BsModalRef();
      component.modalRef.content = {
        stopLoadingSpinner: () => (called = true)
      };
    });

    it('should call stopLoadingSpinner if the request fails', <any>fakeAsync(() => {
      expect(called).toBe(false);
      component._asyncTask('deleteSnapshot', 'rbd/snap/delete', 'someName');
      tick(500);
      expect(called).toBe(true);
    }));
  });

  describe('handling of executing tasks', () => {
    let snapshots: RbdSnapshotModel[];

    const addSnapshot = (name) => {
      const model = new RbdSnapshotModel();
      model.id = 1;
      model.name = name;
      snapshots.push(model);
    };

    const addTask = (task_name: string, snapshot_name: string) => {
      const task = new ExecutingTask();
      task.name = task_name;
      task.metadata = {
        pool_name: 'rbd',
        image_name: 'foo',
        snapshot_name: snapshot_name
      };
      summaryService.addRunningTask(task);
    };

    const expectImageTasks = (snapshot: RbdSnapshotModel, executing: string) => {
      expect(snapshot.cdExecuting).toEqual(executing);
    };

    const refresh = (data) => {
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
      expectImageTasks(component.snapshots[0], undefined);
      expectImageTasks(component.snapshots[1], undefined);
      expectImageTasks(component.snapshots[2], undefined);
      expectImageTasks(component.snapshots[3], 'Creating');
    });

    it('should show when an existing image is being modified', () => {
      addTask('rbd/snap/edit', 'a');
      addTask('rbd/snap/delete', 'b');
      addTask('rbd/snap/rollback', 'c');
      expect(component.snapshots.length).toBe(3);
      expectImageTasks(component.snapshots[0], 'Updating');
      expectImageTasks(component.snapshots[1], 'Deleting');
      expectImageTasks(component.snapshots[2], 'Rolling back');
    });
  });

  describe('snapshot modal dialog', () => {
    beforeEach(() => {
      component.poolName = 'pool01';
      component.rbdName = 'image01';
      spyOn(TestBed.get(BsModalService), 'show').and.callFake((content) => {
        const ref = new BsModalRef();
        ref.content = new content();
        ref.content.onSubmit = new Subject();
        return ref;
      });
    });

    it('should display old snapshot name', () => {
      component.selection.selected = [{ name: 'oldname' }];
      component.selection.update();
      component.openEditSnapshotModal();
      expect(component.modalRef.content.snapName).toBe('oldname');
      expect(component.modalRef.content.editing).toBeTruthy();
    });

    it('should display suggested snapshot name', () => {
      component.openCreateSnapshotModal();
      expect(component.modalRef.content.snapName).toMatch(
        RegExp(`^${component.rbdName}-\\d+T\\d+Z\$`)
      );
    });
  });

  describe('show action buttons and drop down actions depending on permissions', () => {
    let tableActions: TableActionsComponent;
    let scenario: { fn; empty; single };
    let permissionHelper: PermissionHelper;

    const getTableActionComponent = (): TableActionsComponent => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.directive(TableActionsComponent)).componentInstance;
    };

    beforeEach(() => {
      permissionHelper = new PermissionHelper(component.permission, () =>
        getTableActionComponent()
      );
      scenario = {
        fn: () => tableActions.getCurrentButton().name,
        single: 'Rename',
        empty: 'Create'
      };
    });

    describe('with all', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      });

      it(`shows 'Rename' for single selection else 'Create' as main action`, () =>
        permissionHelper.testScenarios(scenario));

      it('shows all actions', () => {
        expect(tableActions.tableActions.length).toBe(8);
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 0);
      });

      it(`shows 'Rename' for single selection else 'Create' as main action`, () =>
        permissionHelper.testScenarios(scenario));

      it(`shows all actions except for 'Delete'`, () => {
        expect(tableActions.tableActions.length).toBe(7);
        component.tableActions.pop();
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 1);
      });

      it(`shows 'Clone' for single selection else 'Create' as main action`, () => {
        scenario.single = 'Clone';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Create', 'Clone', 'Copy' and 'Delete' action`, () => {
        expect(tableActions.tableActions.length).toBe(4);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[4],
          component.tableActions[5],
          component.tableActions[7]
        ]);
      });
    });

    describe('with read, edit and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 1);
      });

      it(`shows always 'Rename' as main action`, () => {
        scenario.empty = 'Rename';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Rename', 'Protect', 'Unprotect', 'Rollback' and 'Delete' action`, () => {
        expect(tableActions.tableActions.length).toBe(5);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[1],
          component.tableActions[2],
          component.tableActions[3],
          component.tableActions[6],
          component.tableActions[7]
        ]);
      });
    });

    describe('with read and create', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 0);
      });

      it(`shows 'Clone' for single selection else 'Create' as main action`, () => {
        scenario.single = 'Clone';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Create', 'Clone' and 'Copy' actions`, () => {
        expect(tableActions.tableActions.length).toBe(3);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[4],
          component.tableActions[5]
        ]);
      });
    });

    describe('with read and edit', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it(`shows always 'Rename' as main action`, () => {
        scenario.empty = 'Rename';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Rename', 'Protect', 'Unprotect' and 'Rollback' actions`, () => {
        expect(tableActions.tableActions.length).toBe(4);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[1],
          component.tableActions[2],
          component.tableActions[3],
          component.tableActions[6]
        ]);
      });
    });

    describe('with read and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 0, 1);
      });

      it(`shows always 'Delete' as main action`, () => {
        scenario.single = 'Delete';
        scenario.empty = 'Delete';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows only 'Delete' action`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[7]]);
      });
    });

    describe('with only read', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 0, 0);
      });

      it('shows no main action', () => {
        permissionHelper.testScenarios({
          fn: () => tableActions.getCurrentButton(),
          single: undefined,
          empty: undefined
        });
      });

      it('shows no actions', () => {
        expect(tableActions.tableActions.length).toBe(0);
        expect(tableActions.tableActions).toEqual([]);
      });
    });

    describe('test unprotected and protected action cases', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it(`shows none of them if nothing is selected`, () => {
        permissionHelper.setSelection([]);
        fixture.detectChanges();
        expect(tableActions.dropDownActions).toEqual([
          component.tableActions[1],
          component.tableActions[6]
        ]);
      });

      it(`shows 'Protect' of them if nothing is selected`, () => {
        permissionHelper.setSelection([{ is_protected: false }]);
        fixture.detectChanges();
        expect(tableActions.dropDownActions).toEqual([
          component.tableActions[1],
          component.tableActions[2],
          component.tableActions[6]
        ]);
      });

      it(`shows 'Unprotect' of them if nothing is selected`, () => {
        permissionHelper.setSelection([{ is_protected: true }]);
        fixture.detectChanges();
        expect(tableActions.dropDownActions).toEqual([
          component.tableActions[1],
          component.tableActions[3],
          component.tableActions[6]
        ]);
      });
    });
  });
});
