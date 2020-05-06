import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef, BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';
import { Subject, throwError as observableThrowError } from 'rxjs';

import {
  configureTestBed,
  expectItemTasks,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { ApiModule } from '../../../shared/api/api.module';
import { RbdService } from '../../../shared/api/rbd.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { DataTableModule } from '../../../shared/datatable/datatable.module';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { Permissions } from '../../../shared/models/permissions';
import { PipesModule } from '../../../shared/pipes/pipes.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { RbdSnapshotFormModalComponent } from '../rbd-snapshot-form/rbd-snapshot-form-modal.component';
import { RbdTabsComponent } from '../rbd-tabs/rbd-tabs.component';
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
    declarations: [RbdSnapshotListComponent, RbdTabsComponent],
    imports: [
      ApiModule,
      BrowserAnimationsModule,
      ComponentsModule,
      DataTableModule,
      HttpClientTestingModule,
      PipesModule,
      RouterTestingModule,
      TabsModule.forRoot(),
      ToastrModule.forRoot()
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
      const i18n = TestBed.inject(I18n);
      const actionLabelsI18n = TestBed.inject(ActionLabelsI18n);
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
        i18n,
        actionLabelsI18n
      );
      spyOn(rbdService, 'deleteSnapshot').and.returnValue(observableThrowError({ status: 500 }));
      spyOn(notificationService, 'notifyTask').and.stub();
      component.modalRef = new BsModalRef();
      component.modalRef.content = {
        stopLoadingSpinner: () => (called = true)
      };
    });

    it('should call stopLoadingSpinner if the request fails', fakeAsync(() => {
      expect(called).toBe(false);
      component._asyncTask('deleteSnapshot', 'rbd/snap/delete', 'someName');
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
      spyOn(TestBed.inject(BsModalService), 'show').and.callFake(() => {
        const ref = new BsModalRef();
        ref.content = new RbdSnapshotFormModalComponent(
          null,
          null,
          null,
          null,
          TestBed.inject(I18n),
          TestBed.inject(ActionLabelsI18n)
        );
        ref.content.onSubmit = new Subject();
        return ref;
      });
    });

    it('should display old snapshot name', () => {
      component.selection.selected = [{ name: 'oldname' }];
      component.openEditSnapshotModal();
      expect(component.modalRef.content.snapName).toBe('oldname');
      expect(component.modalRef.content.editing).toBeTruthy();
    });

    it('should display suggested snapshot name', () => {
      component.openCreateSnapshotModal();
      expect(component.modalRef.content.snapName).toMatch(
        RegExp(`^${component.rbdName}_[\\d-]+T[\\d.:]+[\\+-][\\d:]+$`)
      );
    });
  });

  it('should test all TableActions combinations', () => {
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
});
