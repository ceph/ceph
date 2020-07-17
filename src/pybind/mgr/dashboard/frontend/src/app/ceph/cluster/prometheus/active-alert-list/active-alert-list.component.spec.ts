import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, PermissionHelper } from '../../../../../testing/unit-test-helper';
import { CoreModule } from '../../../../core/core.module';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../../shared/shared.module';
import { CephModule } from '../../../ceph.module';
import { DashboardModule } from '../../../dashboard/dashboard.module';
import { ClusterModule } from '../../cluster.module';
import { ActiveAlertListComponent } from './active-alert-list.component';

describe('ActiveAlertListComponent', () => {
  let component: ActiveAlertListComponent;
  let fixture: ComponentFixture<ActiveAlertListComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      NgbNavModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      SharedModule,
      ClusterModule,
      DashboardModule,
      CephModule,
      CoreModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ActiveAlertListComponent);
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
        actions: ['Create Silence'],
        primary: {
          multiple: 'Create Silence',
          executing: 'Create Silence',
          single: 'Create Silence',
          no: 'Create Silence'
        }
      },
      'create,update': {
        actions: ['Create Silence'],
        primary: {
          multiple: 'Create Silence',
          executing: 'Create Silence',
          single: 'Create Silence',
          no: 'Create Silence'
        }
      },
      'create,delete': {
        actions: ['Create Silence'],
        primary: {
          multiple: 'Create Silence',
          executing: 'Create Silence',
          single: 'Create Silence',
          no: 'Create Silence'
        }
      },
      create: {
        actions: ['Create Silence'],
        primary: {
          multiple: 'Create Silence',
          executing: 'Create Silence',
          single: 'Create Silence',
          no: 'Create Silence'
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
