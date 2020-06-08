import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { TreeModule } from 'angular-tree-component';
import { ToastrModule } from 'ngx-toastr';
import { BehaviorSubject, of } from 'rxjs';

import {
  configureTestBed,
  expectItemTasks,
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

  const refresh = (data: any) => {
    summaryService['summaryDataSource'].next(data);
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      TreeModule,
      ToastrModule.forRoot(),
      NgbNavModule
    ],
    declarations: [IscsiTargetListComponent, IscsiTabsComponent, IscsiTargetDetailsComponent],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.inject(SummaryService);
    iscsiService = TestBed.inject(IscsiService);

    // this is needed because summaryService isn't being reset after each test.
    summaryService['summaryDataSource'] = new BehaviorSubject(null);
    summaryService['summaryData$'] = summaryService['summaryDataSource'].asObservable();

    spyOn(iscsiService, 'status').and.returnValue(of({ available: true }));
    spyOn(iscsiService, 'version').and.returnValue(of({ ceph_iscsi_config_version: 11 }));
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

    const addTarget = (name: string) => {
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
      expectItemTasks(component.targets[0], undefined);
      expectItemTasks(component.targets[1], undefined);
      expectItemTasks(component.targets[2], undefined);
      expectItemTasks(component.targets[3], 'Creating');
    });

    it('should show when an existing target is being modified', () => {
      addTask('iscsi/target/delete', 'iqn.b');
      expect(component.targets.length).toBe(3);
      expectItemTasks(component.targets[1], 'Deleting');
    });
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Create', 'Edit', 'Delete'],
        primary: { multiple: 'Create', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,update': {
        actions: ['Create', 'Edit'],
        primary: { multiple: 'Create', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Delete'],
        primary: { multiple: 'Create', executing: 'Delete', single: 'Delete', no: 'Create' }
      },
      create: {
        actions: ['Create'],
        primary: { multiple: 'Create', executing: 'Create', single: 'Create', no: 'Create' }
      },
      'update,delete': {
        actions: ['Edit', 'Delete'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      update: {
        actions: ['Edit'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
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
