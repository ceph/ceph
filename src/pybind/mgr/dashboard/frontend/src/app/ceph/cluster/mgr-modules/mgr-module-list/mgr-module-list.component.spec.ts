import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { of as observableOf, throwError as observableThrowError } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../../testing/unit-test-helper';
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
      RouterTestingModule,
      SharedModule,
      HttpClientTestingModule,
      TabsModule.forRoot(),
      ToastModule.forRoot()
    ],
    providers: [MgrModuleService, NotificationService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MgrModuleListComponent);
    component = fixture.componentInstance;
    mgrModuleService = TestBed.get(MgrModuleService);
    notificationService = TestBed.get(NotificationService);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
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
        single: 'Edit',
        empty: 'Edit'
      };
    });

    describe('with read and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it('shows action button', () => permissionHelper.testScenarios(scenario));

      it('shows all actions', () => {
        expect(tableActions.tableActions.length).toBe(3);
        expect(tableActions.tableActions).toEqual(component.tableActions);
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
      component.selection.selected.push({
        name: 'foo',
        enabled: false
      });
      component.selection.update();
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
      component.selection.selected.push({
        name: 'bar',
        enabled: true
      });
      component.selection.update();
      component.updateModuleState();
      tick(2000);
      expect(mgrModuleService.disable).toHaveBeenCalledWith('bar');
      expect(mgrModuleService.list).toHaveBeenCalledTimes(1);
      expect(notificationService.suspendToasties).toHaveBeenCalledTimes(2);
      expect(component.blockUI.start).toHaveBeenCalled();
      expect(component.blockUI.stop).toHaveBeenCalled();
      expect(component.table.refreshBtn).toHaveBeenCalled();
    }));
  });
});
