import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { NfsService } from '~/app/shared/api/nfs.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { ExecutingTask } from '~/app/shared/models/executing-task';
import { Summary } from '~/app/shared/models/summary.model';
import { SummaryService } from '~/app/shared/services/summary.service';
import { TaskListService } from '~/app/shared/services/task-list.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, expectItemTasks, PermissionHelper } from '~/testing/unit-test-helper';
import { NfsDetailsComponent } from '../nfs-details/nfs-details.component';
import { NfsListComponent } from './nfs-list.component';
import { SUPPORTED_FSAL } from '../models/nfs.fsal';

describe('NfsListComponent', () => {
  let component: NfsListComponent;
  let fixture: ComponentFixture<NfsListComponent>;
  let summaryService: SummaryService;
  let nfsService: NfsService;
  let httpTesting: HttpTestingController;

  const refresh = (data: Summary) => {
    summaryService['summaryDataSource'].next(data);
  };

  configureTestBed({
    declarations: [NfsListComponent, NfsDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      NgbNavModule,
      ToastrModule.forRoot()
    ],
    providers: [TaskListService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsListComponent);
    component = fixture.componentInstance;
    component.fsal = SUPPORTED_FSAL.CEPH;
    summaryService = TestBed.inject(SummaryService);
    nfsService = TestBed.inject(NfsService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after ngOnInit', () => {
    beforeEach(() => {
      fixture.detectChanges();
      spyOn(nfsService, 'list').and.callThrough();
    });

    afterEach(() => {
      httpTesting.verify();
    });

    it('should load exports on init', () => {
      refresh(new Summary());
      httpTesting.expectOne('api/nfs-ganesha/export');
      expect(nfsService.list).toHaveBeenCalled();
    });

    it('should not load images on init because no data', () => {
      refresh(undefined);
      expect(nfsService.list).not.toHaveBeenCalled();
    });

    it('should call error function on init when summary service fails', () => {
      spyOn(component.table, 'reset');
      summaryService['summaryDataSource'].error(undefined);
      expect(component.table.reset).toHaveBeenCalled();
    });
  });

  describe('handling of executing tasks', () => {
    let exports: any[];

    const addExport = (export_id: string) => {
      const model = {
        export_id: export_id,
        path: 'path_' + export_id,
        fsal: {
          name: 'CEPH'
        },
        cluster_id: 'cluster_' + export_id
      };
      exports.push(model);
    };

    const addTask = (name: string, export_id: string) => {
      const task = new ExecutingTask();
      task.name = name;
      switch (task.name) {
        case 'nfs/create':
          task.metadata = {
            path: 'path_' + export_id,
            fsal: {
              name: 'CEPH'
            },
            cluster_id: 'cluster_' + export_id
          };
          break;
        default:
          task.metadata = {
            cluster_id: 'cluster_' + export_id,
            export_id: export_id
          };
          break;
      }
      summaryService.addRunningTask(task);
    };

    beforeEach(() => {
      exports = [];
      addExport('a');
      addExport('b');
      addExport('c');
      component.exports = exports;
      refresh(new Summary());
      spyOn(nfsService, 'list').and.callFake(() => of(exports));
      fixture.detectChanges();
    });

    it('should gets all exports without tasks', () => {
      expect(component.exports.length).toBe(3);
      expect(component.exports.every((expo) => !expo.cdExecuting)).toBeTruthy();
    });

    it('should add a new export from a task', fakeAsync(() => {
      addTask('nfs/create', 'd');
      tick();
      expect(component.exports.length).toBe(4);
      expectItemTasks(component.exports[0], undefined);
      expectItemTasks(component.exports[1], undefined);
      expectItemTasks(component.exports[2], undefined);
      expectItemTasks(component.exports[3], 'Creating');
    }));

    it('should show when an existing export is being modified', () => {
      addTask('nfs/edit', 'a');
      addTask('nfs/delete', 'b');
      expect(component.exports.length).toBe(3);
      expectItemTasks(component.exports[0], 'Updating');
      expectItemTasks(component.exports[1], 'Deleting');
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
