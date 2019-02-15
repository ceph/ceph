import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TreeModule } from 'ng2-tree';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { BehaviorSubject, of } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { IscsiService } from '../../../shared/api/iscsi.service';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTabsComponent } from '../iscsi-tabs/iscsi-tabs.component';
import { IscsiTargetDetailsComponent } from '../iscsi-target-details/iscsi-target-details.component';
import { IscsiTargetListComponent } from './iscsi-target-list.component';

describe('IscsiTargetListComponent', () => {
  let component: IscsiTargetListComponent;
  let fixture: ComponentFixture<IscsiTargetListComponent>;
  let summaryService: SummaryService;
  let iscsiService: IscsiService;

  const refresh = (data) => {
    summaryService['summaryDataSource'].next(data);
  };

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      TabsModule.forRoot(),
      TreeModule,
      ToastModule.forRoot()
    ],
    declarations: [IscsiTargetListComponent, IscsiTabsComponent, IscsiTargetDetailsComponent],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.get(SummaryService);
    iscsiService = TestBed.get(IscsiService);

    // this is needed because summaryService isn't being reset after each test.
    summaryService['summaryDataSource'] = new BehaviorSubject(null);
    summaryService['summaryData$'] = summaryService['summaryDataSource'].asObservable();

    spyOn(iscsiService, 'status').and.returnValue(of({ available: true }));
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after ngOnInit', () => {
    beforeEach(() => {
      spyOn(iscsiService, 'listTargets').and.callThrough();
      fixture.detectChanges();
    });

    it('should load targets on init', () => {
      refresh({});
      expect(iscsiService.status).toHaveBeenCalled();
      expect(iscsiService.listTargets).toHaveBeenCalled();
    });

    it('should not load targets on init because no data', () => {
      refresh(undefined);
      expect(iscsiService.listTargets).not.toHaveBeenCalled();
    });

    it('should call error function on init when summary service fails', () => {
      spyOn(component.table, 'reset');
      summaryService['summaryDataSource'].error(undefined);
      expect(component.table.reset).toHaveBeenCalled();
    });
  });

  describe('handling of executing tasks', () => {
    let targets: any[];

    const addTarget = (name) => {
      const model: any = {
        target_iqn: name,
        portals: [{ host: 'node1', ip: '192.168.100.201' }],
        disks: [{ pool: 'rbd', image: 'disk_1', controls: {} }],
        clients: [
          {
            client_iqn: 'iqn.1994-05.com.redhat:rh7-client',
            luns: [{ pool: 'rbd', image: 'disk_1' }],
            auth: {
              user: 'myiscsiusername',
              password: 'myiscsipassword',
              mutual_user: null,
              mutual_password: null
            }
          }
        ],
        groups: [],
        target_controls: {}
      };
      targets.push(model);
    };

    const addTask = (name: string, target_iqn: string) => {
      const task = new ExecutingTask();
      task.name = name;
      switch (task.name) {
        case 'iscsi/target/create':
          task.metadata = {
            target_iqn: target_iqn
          };
          break;
        case 'iscsi/target/delete':
          task.metadata = {
            target_iqn: target_iqn
          };
          break;
        default:
          task.metadata = {
            target_iqn: target_iqn
          };
          break;
      }
      summaryService.addRunningTask(task);
    };

    const expectTargetTasks = (target: any, executing: string) => {
      expect(target.cdExecuting).toEqual(executing);
    };

    beforeEach(() => {
      targets = [];
      addTarget('iqn.a');
      addTarget('iqn.b');
      addTarget('iqn.c');

      component.targets = targets;
      refresh({ executing_tasks: [], finished_tasks: [] });
      spyOn(iscsiService, 'listTargets').and.callFake(() => of(targets));
      fixture.detectChanges();
    });

    it('should gets all targets without tasks', () => {
      expect(component.targets.length).toBe(3);
      expect(component.targets.every((target) => !target.cdExecuting)).toBeTruthy();
    });

    it('should add a new target from a task', () => {
      addTask('iscsi/target/create', 'iqn.d');
      expect(component.targets.length).toBe(4);
      expectTargetTasks(component.targets[0], undefined);
      expectTargetTasks(component.targets[1], undefined);
      expectTargetTasks(component.targets[2], undefined);
      expectTargetTasks(component.targets[3], 'Creating');
    });

    it('should show when an existing target is being modified', () => {
      addTask('iscsi/target/delete', 'iqn.b');
      expect(component.targets.length).toBe(3);
      expectTargetTasks(component.targets[1], 'Deleting');
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
      permissionHelper = new PermissionHelper(component.permissions.iscsi, () =>
        getTableActionComponent()
      );
      scenario = {
        fn: () => tableActions.getCurrentButton().name,
        single: 'Edit',
        empty: 'Add'
      };
    });

    describe('with all', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      });

      it(`shows 'Edit' for single selection else 'Add' as main action`, () => {
        permissionHelper.testScenarios(scenario);
      });

      it('shows all actions', () => {
        expect(tableActions.tableActions.length).toBe(3);
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 0);
        scenario.single = 'Edit';
      });

      it(`should always show 'Edit'`, () => {
        permissionHelper.testScenarios(scenario);
      });

      it(`shows all actions except for 'Delete'`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        component.tableActions.pop();
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 1);
      });

      it(`shows 'Delete' for single selection else 'Add' as main action`, () => {
        scenario.single = 'Delete';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Add' and 'Delete' actions`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[2]
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

      it(`shows 'Edit' and 'Delete' actions`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[1],
          component.tableActions[2]
        ]);
      });
    });

    describe('with read and create', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 0);
      });

      it(`shows 'Add' for single selection and 'Add' as main action`, () => {
        scenario.single = 'Add';
        permissionHelper.testScenarios(scenario);
      });

      it(`shows 'Add' actions`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[0]]);
      });
    });

    describe('with read and edit', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it(`shows no actions`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[1]]);
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

      it(`shows 'Delete' actions`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[2]]);
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
