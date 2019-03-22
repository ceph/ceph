import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule } from 'ngx-bootstrap/tabs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../../testing/unit-test-helper';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../../shared/shared.module';
import { PrometheusTabsComponent } from '../prometheus-tabs/prometheus-tabs.component';
import { AlertListComponent } from './alert-list.component';

describe('PrometheusListComponent', () => {
  let component: AlertListComponent;
  let fixture: ComponentFixture<AlertListComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      RouterTestingModule,
      ToastModule.forRoot(),
      SharedModule
    ],
    declarations: [AlertListComponent, PrometheusTabsComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AlertListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('show action buttons and drop down actions depending on permissions', () => {
    let tableActions: TableActionsComponent;
    let scenario: { fn; empty; single };
    let permissionHelper: PermissionHelper;
    let combinations: number[][];

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
        single: 'Create silence',
        empty: 'Create silence'
      };
      tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
    });

    const permissionSwitch = (combination) => {
      tableActions = permissionHelper.setPermissionsAndGetActions(
        combination[0],
        combination[1],
        combination[2]
      );
      tableActions.tableActions = component.tableActions;
      tableActions.ngOnInit();
    };

    const testCombinations = (test: Function) => {
      combinations.forEach((combination) => {
        permissionSwitch(combination);
        test();
      });
    };

    describe('with every permission combination that includes create', () => {
      beforeEach(() => {
        combinations = [[1, 1, 1], [1, 1, 0], [1, 0, 1], [1, 0, 0]];
      });

      it(`always shows 'Create silence' as main action`, () => {
        testCombinations(() => permissionHelper.testScenarios(scenario));
      });

      it('shows all actions', () => {
        testCombinations(() => {
          expect(tableActions.tableActions.length).toBe(1);
          expect(tableActions.tableActions).toEqual(component.tableActions);
        });
      });
    });

    describe('with every permission combination that does not include create', () => {
      beforeEach(() => {
        combinations = [[0, 1, 1], [0, 1, 0], [0, 0, 1], [0, 0, 0]];
      });

      it(`won't show any action`, () => {
        testCombinations(() => {
          permissionHelper.testScenarios({
            fn: () => tableActions.getCurrentButton(),
            single: undefined,
            empty: undefined
          });
        });
      });

      it('shows no actions', () => {
        testCombinations(() => {
          expect(tableActions.tableActions.length).toBe(0);
          expect(tableActions.tableActions).toEqual([]);
        });
      });
    });
  });
});
