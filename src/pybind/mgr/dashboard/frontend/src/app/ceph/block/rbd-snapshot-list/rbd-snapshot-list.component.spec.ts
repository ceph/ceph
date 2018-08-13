import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, ModalModule } from 'ngx-bootstrap';
import { throwError as observableThrowError } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ApiModule } from '../../../shared/api/api.module';
import { RbdService } from '../../../shared/api/rbd.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { DataTableModule } from '../../../shared/datatable/datatable.module';
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
      ModalModule.forRoot(),
      ToastModule.forRoot(),
      ServicesModule,
      ApiModule,
      HttpClientTestingModule,
      RouterTestingModule,
      PipesModule
    ],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      SummaryService,
      TaskListService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.get(SummaryService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('api delete request', () => {
    let called;
    let rbdService: RbdService;
    let notificationService: NotificationService;
    let authStorageService: AuthStorageService;

    beforeEach(() => {
      called = false;
      rbdService = new RbdService(null);
      notificationService = new NotificationService(null, null);
      authStorageService = new AuthStorageService();
      authStorageService.set('user', { 'rbd-image': ['create', 'read', 'update', 'delete'] });
      component = new RbdSnapshotListComponent(
        authStorageService,
        null,
        null,
        null,
        rbdService,
        null,
        notificationService,
        null,
        null
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
});
