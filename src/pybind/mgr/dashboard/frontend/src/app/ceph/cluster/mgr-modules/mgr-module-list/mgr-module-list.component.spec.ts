import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf, throwError as observableThrowError } from 'rxjs';

import { configureTestBed, PermissionHelper } from '../../../../../testing/unit-test-helper';
import { MgrModuleService } from '../../../../shared/api/mgr-module.service';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { NotificationService } from '../../../../shared/services/notification.service';
import { SharedModule } from '../../../../shared/shared.module';
import { MgrModuleDetailsComponent } from '../mgr-module-details/mgr-module-details.component';
import { MgrModuleListComponent } from './mgr-module-list.component';

describe('MgrModuleListComponent', () => {
  let component: MgrModuleListComponent;
  let fixture: ComponentFixture<MgrModuleListComponent>;
  let mgrModuleService: MgrModuleService;
  let notificationService: NotificationService;

  configureTestBed({
    declarations: [MgrModuleListComponent, MgrModuleDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      RouterTestingModule,
      SharedModule,
      HttpClientTestingModule,
      NgbNavModule,
      ToastrModule.forRoot()
    ],
    providers: [MgrModuleService, NotificationService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MgrModuleListComponent);
    component = fixture.componentInstance;
    mgrModuleService = TestBed.inject(MgrModuleService);
    notificationService = TestBed.inject(NotificationService);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Edit', 'Enable', 'Disable'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      'create,update': {
        actions: ['Edit', 'Enable', 'Disable'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      'create,delete': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      },
      create: { actions: [], primary: { multiple: '', executing: '', single: '', no: '' } },
      'update,delete': {
        actions: ['Edit', 'Enable', 'Disable'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      update: {
        actions: ['Edit', 'Enable', 'Disable'],
        primary: { multiple: 'Edit', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      delete: { actions: [], primary: { multiple: '', executing: '', single: '', no: '' } },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });

  describe('should update module state', () => {
    beforeEach(() => {
      component.selection = new CdTableSelection();
      spyOn(notificationService, 'suspendToasties');
      spyOn(component.blockUI, 'start');
      spyOn(component.blockUI, 'stop');
      spyOn(component.table, 'refreshBtn');
    });

    it('should enable module', fakeAsync(() => {
      spyOn(mgrModuleService, 'enable').and.returnValue(observableThrowError('y'));
      spyOn(mgrModuleService, 'list').and.returnValues(observableThrowError('z'), observableOf([]));
      component.selection.add({
        name: 'foo',
        enabled: false,
        always_on: false
      });
      component.updateModuleState();
      tick(2000);
      tick(2000);
      expect(mgrModuleService.enable).toHaveBeenCalledWith('foo');
      expect(mgrModuleService.list).toHaveBeenCalledTimes(2);
      expect(notificationService.suspendToasties).toHaveBeenCalledTimes(2);
      expect(component.blockUI.start).toHaveBeenCalled();
      expect(component.blockUI.stop).toHaveBeenCalled();
      expect(component.table.refreshBtn).toHaveBeenCalled();
    }));

    it('should disable module', fakeAsync(() => {
      spyOn(mgrModuleService, 'disable').and.returnValue(observableThrowError('x'));
      spyOn(mgrModuleService, 'list').and.returnValue(observableOf([]));
      component.selection.add({
        name: 'bar',
        enabled: true,
        always_on: false
      });
      component.updateModuleState();
      tick(2000);
      expect(mgrModuleService.disable).toHaveBeenCalledWith('bar');
      expect(mgrModuleService.list).toHaveBeenCalledTimes(1);
      expect(notificationService.suspendToasties).toHaveBeenCalledTimes(2);
      expect(component.blockUI.start).toHaveBeenCalled();
      expect(component.blockUI.stop).toHaveBeenCalled();
      expect(component.table.refreshBtn).toHaveBeenCalled();
    }));

    it('should not disable module (1)', () => {
      component.selection.selected = [
        {
          name: 'dashboard'
        }
      ];
      expect(component.isTableActionDisabled('enabled')).toBeTruthy();
    });

    it('should not disable module (2)', () => {
      component.selection.selected = [
        {
          name: 'bar',
          always_on: true
        }
      ];
      expect(component.isTableActionDisabled('enabled')).toBeTruthy();
    });
  });
});
