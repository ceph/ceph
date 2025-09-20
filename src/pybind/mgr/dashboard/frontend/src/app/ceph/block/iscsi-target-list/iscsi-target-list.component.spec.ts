import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { BehaviorSubject, of } from 'rxjs';

import { IscsiService } from '~/app/shared/api/iscsi.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, expectItemTasks, PermissionHelper } from '~/testing/unit-test-helper';
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
      ToastrModule.forRoot(),
      NgbNavModule
    ],
    declarations: [IscsiTargetListComponent, IscsiTabsComponent, IscsiTargetDetailsComponent],
    providers: [TaskListService]
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
    spyOn(component, 'setTableRefreshTimeout').and.stub();
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

    it('should call settings on the getTargets methods', () => {
      spyOn(iscsiService, 'settings').and.callThrough();
      component.getTargets();
      expect(iscsiService.settings).toHaveBeenCalled();
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

  describe('handling of actions', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    let action: CdTableAction;

    const getAction = (name: string): CdTableAction => {
      return component.tableActions.find((tableAction) => tableAction.name === name);
    };

    describe('edit', () => {
      beforeEach(() => {
        action = getAction('Edit');
      });

      it('should be disabled if no gateways', () => {
        component.selection.selected = [
          {
            id: '-1'
          }
        ];
        expect(action.disable(undefined)).toBe('Unavailable gateway(s)');
      });

      it('should be enabled if active sessions', () => {
        component.selection.selected = [
          {
            id: '-1',
            info: {
              num_sessions: 1
            }
          }
        ];
        expect(action.disable(undefined)).toBeFalsy();
      });

      it('should be enabled if no active sessions', () => {
        component.selection.selected = [
          {
            id: '-1',
            info: {
              num_sessions: 0
            }
          }
        ];
        expect(action.disable(undefined)).toBeFalsy();
      });
    });

    describe('delete', () => {
      beforeEach(() => {
        action = getAction('Delete');
      });

      it('should be disabled if no gateways', () => {
        component.selection.selected = [
          {
            id: '-1'
          }
        ];
        expect(action.disable(undefined)).toBe('Unavailable gateway(s)');
      });

      it('should be disabled if active sessions', () => {
        component.selection.selected = [
          {
            id: '-1',
            info: {
              num_sessions: 1
            }
          }
        ];
        expect(action.disable(undefined)).toBe('Target has active sessions');
      });

      it('should be enabled if no active sessions', () => {
        component.selection.selected = [
          {
            id: '-1',
            info: {
              num_sessions: 0
            }
          }
        ];
        expect(action.disable(undefined)).toBeFalsy();
      });
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
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'create,update': {
        actions: ['Create', 'Edit'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'create,delete': {
        actions: ['Create', 'Delete'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      create: {
        actions: ['Create'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'update,delete': {
        actions: ['Edit', 'Delete'],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      },
      update: {
        actions: ['Edit'],
        primary: {
          multiple: 'Edit',
          executing: 'Edit',
          single: 'Edit',
          no: 'Edit'
        }
      },
      delete: {
        actions: ['Delete'],
        primary: {
          multiple: 'Delete',
          executing: 'Delete',
          single: 'Delete',
          no: 'Delete'
        }
      },
      'no-permissions': {
        actions: [],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      }
    });
  });
});
