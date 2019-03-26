import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { AlertModule } from 'ngx-bootstrap/alert';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { TooltipModule } from 'ngx-bootstrap/tooltip';
import { BehaviorSubject, of } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { RbdService } from '../../../shared/api/rbd.service';
import { ActionLabels } from '../../../shared/constants/app.constants';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdConfigurationListComponent } from '../rbd-configuration-list/rbd-configuration-list.component';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdListComponent } from './rbd-list.component';
import { RbdModel } from './rbd-model';

describe('RbdListComponent', () => {
  let fixture: ComponentFixture<RbdListComponent>;
  let component: RbdListComponent;
  let summaryService: SummaryService;
  let rbdService: RbdService;

  const refresh = (data) => {
    summaryService['summaryDataSource'].next(data);
  };

  configureTestBed({
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ModalModule.forRoot(),
      TooltipModule.forRoot(),
      ToastModule.forRoot(),
      AlertModule.forRoot(),
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [
      RbdListComponent,
      RbdDetailsComponent,
      RbdSnapshotListComponent,
      RbdConfigurationListComponent
    ],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.get(SummaryService);
    rbdService = TestBed.get(RbdService);

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

  describe('handling of executing tasks', () => {
    let images: RbdModel[];

    const addImage = (name) => {
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
            dest_image_name: 'd'
          };
          break;
        case 'rbd/clone':
          task.metadata = {
            child_pool_name: 'rbd',
            child_image_name: 'd'
          };
          break;
        default:
          task.metadata = {
            pool_name: 'rbd',
            image_name: image_name
          };
          break;
      }
      summaryService.addRunningTask(task);
    };

    const expectImageTasks = (image: RbdModel, executing: string) => {
      expect(image.cdExecuting).toEqual(executing);
    };

    beforeEach(() => {
      images = [];
      addImage('a');
      addImage('b');
      addImage('c');
      component.images = images;
      refresh({ executing_tasks: [], finished_tasks: [] });
      spyOn(rbdService, 'list').and.callFake(() =>
        of([{ poool_name: 'rbd', status: 1, value: images }])
      );
      fixture.detectChanges();
    });

    it('should gets all images without tasks', () => {
      expect(component.images.length).toBe(3);
      expect(component.images.every((image) => !image.cdExecuting)).toBeTruthy();
    });

    it('should add a new image from a task', () => {
      addTask('rbd/create', 'd');
      expect(component.images.length).toBe(4);
      expectImageTasks(component.images[0], undefined);
      expectImageTasks(component.images[1], undefined);
      expectImageTasks(component.images[2], undefined);
      expectImageTasks(component.images[3], 'Creating');
    });

    it('should show when a image is being cloned', () => {
      addTask('rbd/clone', 'd');
      expect(component.images.length).toBe(4);
      expectImageTasks(component.images[0], undefined);
      expectImageTasks(component.images[1], undefined);
      expectImageTasks(component.images[2], undefined);
      expectImageTasks(component.images[3], 'Cloning');
    });

    it('should show when a image is being copied', () => {
      addTask('rbd/copy', 'd');
      expect(component.images.length).toBe(4);
      expectImageTasks(component.images[0], undefined);
      expectImageTasks(component.images[1], undefined);
      expectImageTasks(component.images[2], undefined);
      expectImageTasks(component.images[3], 'Copying');
    });

    it('should show when an existing image is being modified', () => {
      addTask('rbd/edit', 'a');
      addTask('rbd/delete', 'b');
      addTask('rbd/flatten', 'c');
      expect(component.images.length).toBe(3);
      expectImageTasks(component.images[0], 'Updating');
      expectImageTasks(component.images[1], 'Deleting');
      expectImageTasks(component.images[2], 'Flattening');
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
        single: ActionLabels.EDIT,
        empty: ActionLabels.CREATE
      };
    });

    describe('with all', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      });

      it(`shows 'Edit' for single selection else 'Add' as main action`, () =>
        permissionHelper.testScenarios(scenario));

      it('shows all actions', () => {
        expect(tableActions.tableActions.length).toBe(6);
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 0);
      });

      it(`shows 'Edit' for single selection else 'Add' as main action`, () =>
        permissionHelper.testScenarios(scenario));

      it(`shows all actions except for 'Delete' and 'Move'`, () => {
        expect(tableActions.tableActions.length).toBe(4);
        component.tableActions.pop();
        component.tableActions.pop();
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 1);
      });

      it(`shows 'Copy' for single selection else 'Add' as main action`, () => {
        scenario.single = 'Copy';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Add', 'Copy', 'Delete' and 'Move' action`, () => {
        expect(tableActions.tableActions.length).toBe(4);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[2],
          component.tableActions[4],
          component.tableActions[5]
        ]);
      });
    });

    describe('with read, edit and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 1);
      });

      it(`shows always 'Edit' as main action`, () => {
        scenario.empty = 'Edit';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Edit', 'Flatten', 'Delete' and 'Move' action`, () => {
        expect(tableActions.tableActions.length).toBe(4);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[1],
          component.tableActions[3],
          component.tableActions[4],
          component.tableActions[5]
        ]);
      });
    });

    describe('with read and create', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 0);
      });

      it(`shows 'Copy' for single selection else 'Add' as main action`, () => {
        scenario.single = 'Copy';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Copy' and 'Add' actions`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[2]
        ]);
      });
    });

    describe('with read and edit', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it(`shows always 'Edit' as main action`, () => {
        scenario.empty = 'Edit';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Edit' and 'Flatten' actions`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[1],
          component.tableActions[3]
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

      it(`shows 'Delete' and 'Move' actions`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[4],
          component.tableActions[5]
        ]);
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
  });
});
