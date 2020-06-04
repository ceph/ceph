import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { AlertModule } from 'ngx-bootstrap/alert';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { TooltipModule } from 'ngx-bootstrap/tooltip';
import { ToastrModule } from 'ngx-toastr';
import { BehaviorSubject, of } from 'rxjs';

import {
  configureTestBed,
  expectItemTasks,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { RbdService } from '../../../shared/api/rbd.service';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationListComponent } from '../rbd-configuration-list/rbd-configuration-list.component';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdTabsComponent } from '../rbd-tabs/rbd-tabs.component';
import { RbdListComponent } from './rbd-list.component';
import { RbdModel } from './rbd-model';

describe('RbdListComponent', () => {
  let fixture: ComponentFixture<RbdListComponent>;
  let component: RbdListComponent;
  let summaryService: SummaryService;
  let rbdService: RbdService;

  const refresh = (data: any) => {
    summaryService['summaryDataSource'].next(data);
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      BsDropdownModule.forRoot(),
      NgbNavModule,
      ModalModule.forRoot(),
      TooltipModule.forRoot(),
      ToastrModule.forRoot(),
      AlertModule.forRoot(),
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [
      RbdListComponent,
      RbdDetailsComponent,
      RbdSnapshotListComponent,
      RbdConfigurationListComponent,
      RbdTabsComponent
    ],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.inject(SummaryService);
    rbdService = TestBed.inject(RbdService);

    // this is needed because summaryService isn't being reset after each test.
    summaryService['summaryDataSource'] = new BehaviorSubject(null);
    summaryService['summaryData$'] = summaryService['summaryDataSource'].asObservable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after ngOnInit', () => {
    beforeEach(() => {
      fixture.detectChanges();
      spyOn(rbdService, 'list').and.callThrough();
    });

    it('should load images on init', () => {
      refresh({});
      expect(rbdService.list).toHaveBeenCalled();
    });

    it('should not load images on init because no data', () => {
      refresh(undefined);
      expect(rbdService.list).not.toHaveBeenCalled();
    });

    it('should call error function on init when summary service fails', () => {
      spyOn(component.table, 'reset');
      summaryService['summaryDataSource'].error(undefined);
      expect(component.table.reset).toHaveBeenCalled();
      expect(component.viewCacheStatusList).toEqual([{ status: ViewCacheStatus.ValueException }]);
    });
  });

  describe('handling of deletion', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    it('should check if there are no snapshots', () => {
      component.selection.add({
        id: '-1',
        name: 'rbd1',
        pool_name: 'rbd'
      });
      expect(component.hasSnapshots()).toBeFalsy();
    });

    it('should check if there are snapshots', () => {
      component.selection.add({
        id: '-1',
        name: 'rbd1',
        pool_name: 'rbd',
        snapshots: [{}, {}]
      });
      expect(component.hasSnapshots()).toBeTruthy();
    });

    it('should get delete disable description', () => {
      component.selection.add({
        id: '-1',
        name: 'rbd1',
        pool_name: 'rbd',
        snapshots: [
          {
            children: [{}]
          }
        ]
      });
      expect(component.getDeleteDisableDesc()).toBe(
        'This RBD has cloned snapshots. Please delete related RBDs before deleting this RBD.'
      );
    });

    it('should list all protected snapshots', () => {
      component.selection.add({
        id: '-1',
        name: 'rbd1',
        pool_name: 'rbd',
        snapshots: [
          {
            name: 'snap1',
            is_protected: false
          },
          {
            name: 'snap2',
            is_protected: true
          }
        ]
      });

      expect(component.listProtectedSnapshots()).toEqual(['snap2']);
    });
  });

  describe('handling of executing tasks', () => {
    let images: RbdModel[];

    const addImage = (name: string) => {
      const model = new RbdModel();
      model.id = '-1';
      model.name = name;
      model.pool_name = 'rbd';
      images.push(model);
    };

    const addTask = (name: string, image_name: string) => {
      const task = new ExecutingTask();
      task.name = name;
      switch (task.name) {
        case 'rbd/copy':
          task.metadata = {
            dest_pool_name: 'rbd',
            dest_namespace: null,
            dest_image_name: 'd'
          };
          break;
        case 'rbd/clone':
          task.metadata = {
            child_pool_name: 'rbd',
            child_namespace: null,
            child_image_name: 'd'
          };
          break;
        case 'rbd/create':
          task.metadata = {
            pool_name: 'rbd',
            namespace: null,
            image_name: image_name
          };
          break;
        default:
          task.metadata = {
            image_spec: `rbd/${image_name}`
          };
          break;
      }
      summaryService.addRunningTask(task);
    };

    beforeEach(() => {
      images = [];
      addImage('a');
      addImage('b');
      addImage('c');
      component.images = images;
      refresh({ executing_tasks: [], finished_tasks: [] });
      spyOn(rbdService, 'list').and.callFake(() =>
        of([{ pool_name: 'rbd', status: 1, value: images }])
      );
      fixture.detectChanges();
    });

    it('should gets all images without tasks', () => {
      expect(component.images.length).toBe(3);
      expect(component.images.every((image: any) => !image.cdExecuting)).toBeTruthy();
    });

    it('should add a new image from a task', () => {
      addTask('rbd/create', 'd');
      expect(component.images.length).toBe(4);
      expectItemTasks(component.images[0], undefined);
      expectItemTasks(component.images[1], undefined);
      expectItemTasks(component.images[2], undefined);
      expectItemTasks(component.images[3], 'Creating');
    });

    it('should show when a image is being cloned', () => {
      addTask('rbd/clone', 'd');
      expect(component.images.length).toBe(4);
      expectItemTasks(component.images[0], undefined);
      expectItemTasks(component.images[1], undefined);
      expectItemTasks(component.images[2], undefined);
      expectItemTasks(component.images[3], 'Cloning');
    });

    it('should show when a image is being copied', () => {
      addTask('rbd/copy', 'd');
      expect(component.images.length).toBe(4);
      expectItemTasks(component.images[0], undefined);
      expectItemTasks(component.images[1], undefined);
      expectItemTasks(component.images[2], undefined);
      expectItemTasks(component.images[3], 'Copying');
    });

    it('should show when an existing image is being modified', () => {
      addTask('rbd/edit', 'a');
      addTask('rbd/delete', 'b');
      addTask('rbd/flatten', 'c');
      expect(component.images.length).toBe(3);
      expectItemTasks(component.images[0], 'Updating');
      expectItemTasks(component.images[1], 'Deleting');
      expectItemTasks(component.images[2], 'Flattening');
    });
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Create', 'Edit', 'Copy', 'Flatten', 'Delete', 'Move to Trash'],
        primary: { multiple: 'Create', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,update': {
        actions: ['Create', 'Edit', 'Copy', 'Flatten'],
        primary: { multiple: 'Create', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Copy', 'Delete', 'Move to Trash'],
        primary: { multiple: 'Create', executing: 'Copy', single: 'Copy', no: 'Create' }
      },
      create: {
        actions: ['Create', 'Copy'],
        primary: { multiple: 'Create', executing: 'Copy', single: 'Copy', no: 'Create' }
      },
      'update,delete': {
        actions: ['Edit', 'Flatten', 'Delete', 'Move to Trash'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      update: {
        actions: ['Edit', 'Flatten'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      delete: {
        actions: ['Delete', 'Move to Trash'],
        primary: { multiple: 'Delete', executing: 'Delete', single: 'Delete', no: 'Delete' }
      },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });
});
