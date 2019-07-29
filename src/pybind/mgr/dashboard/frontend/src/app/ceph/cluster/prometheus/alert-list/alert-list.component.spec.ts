import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../../testing/unit-test-helper';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../../shared/shared.module';
import { PrometheusTabsComponent } from '../prometheus-tabs/prometheus-tabs.component';
import { AlertListComponent } from './alert-list.component';

describe('AlertListComponent', () => {
  let component: AlertListComponent;
  let fixture: ComponentFixture<AlertListComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      RouterTestingModule,
      ToastrModule.forRoot(),
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

  it('should test all TableActions combinations', () => {
    component.ngOnInit();
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permission);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Create silence'],
        primary: {
          multiple: 'Create silence',
          executing: 'Create silence',
          single: 'Create silence',
          no: 'Create silence'
        }
      },
      'create,update': {
        actions: ['Create silence'],
        primary: {
          multiple: 'Create silence',
          executing: 'Create silence',
          single: 'Create silence',
          no: 'Create silence'
        }
      },
      'create,delete': {
        actions: ['Create silence'],
        primary: {
          multiple: 'Create silence',
          executing: 'Create silence',
          single: 'Create silence',
          no: 'Create silence'
        }
      },
      create: {
        actions: ['Create silence'],
        primary: {
          multiple: 'Create silence',
          executing: 'Create silence',
          single: 'Create silence',
          no: 'Create silence'
        }
      },
      'update,delete': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      },
      update: { actions: [], primary: { multiple: '', executing: '', single: '', no: '' } },
      delete: { actions: [], primary: { multiple: '', executing: '', single: '', no: '' } },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });
});
