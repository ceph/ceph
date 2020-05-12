import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import {
  configureTestBed,
  expectItemTasks,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { NfsService } from '../../../shared/api/nfs.service';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ExecutingTask } from '../../../shared/models/executing-task';
import { Summary } from '../../../shared/models/summary.model';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskListService } from '../../../shared/services/task-list.service';
import { SharedModule } from '../../../shared/shared.module';
import { NfsDetailsComponent } from '../nfs-details/nfs-details.component';
import { NfsListComponent } from './nfs-list.component';

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
      ToastrModule.forRoot(),
      TabsModule.forRoot()
    ],
    providers: [TaskListService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsListComponent);
    component = fixture.componentInstance;
    summaryService = TestBed.get(SummaryService);
    nfsService = TestBed.get(NfsService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after ngOnInit', () => {
    beforeEach(() => {
      fixture.detectChanges();
      spyOn(nfsService, 'list').and.callThrough();
      httpTesting.expectOne('api/nfs-ganesha/daemon').flush([]);
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
        fsal: 'fsal_' + export_id,
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
            fsal: 'fsal_' + export_id,
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

      const req = httpTesting.expectOne('api/nfs-ganesha/daemon');
      req.flush([]);
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
