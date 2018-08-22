import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, PermissionHelper } from '../../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../../../shared/components/components.module';
import { DataTableModule } from '../../../../shared/datatable/datatable.module';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { Permissions } from '../../../../shared/models/permissions';
import { DimlessPipe } from '../../../../shared/pipes/dimless.pipe';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { FormatterService } from '../../../../shared/services/formatter.service';
import { SharedModule } from '../../../../shared/shared.module';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import { OsdDetailsComponent } from '../osd-details/osd-details.component';
import { OsdPerformanceHistogramComponent } from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdListComponent } from './osd-list.component';

describe('OsdListComponent', () => {
  let component: OsdListComponent;
  let fixture: ComponentFixture<OsdListComponent>;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ osd: ['read', 'update', 'create', 'delete'] });
    }
  };

  configureTestBed({
    imports: [
      HttpClientModule,
      PerformanceCounterModule,
      TabsModule.forRoot(),
      DataTableModule,
      ComponentsModule,
      SharedModule
    ],
    declarations: [OsdListComponent, OsdDetailsComponent, OsdPerformanceHistogramComponent],
    providers: [
      DimlessPipe,
      FormatterService,
      { provide: AuthStorageService, useValue: fakeAuthStorageService }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdListComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('show table actions as defined', () => {
    let tableActions: TableActionsComponent;
    let scenario: { fn; empty; single };
    let permissionHelper: PermissionHelper;

    const getTableActionComponent = () => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.directive(TableActionsComponent)).componentInstance;
    };

    beforeEach(() => {
      permissionHelper = new PermissionHelper(component.permission, () =>
        getTableActionComponent()
      );
      scenario = {
        fn: () => tableActions.getCurrentButton(),
        single: undefined,
        empty: undefined
      };
      tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
    });

    it('shows no action button', () => permissionHelper.testScenarios(scenario));

    it('shows all actions', () => {
      expect(tableActions.tableActions.length).toBe(2);
      expect(tableActions.tableActions).toEqual(component.tableActions);
    });

    it(`shows 'Perform task' as drop down`, () => {
      expect(
        fixture.debugElement.query(By.directive(TableActionsComponent)).query(By.css('button'))
          .nativeElement.textContent
      ).toBe('Perform Task');
    });
  });
});
