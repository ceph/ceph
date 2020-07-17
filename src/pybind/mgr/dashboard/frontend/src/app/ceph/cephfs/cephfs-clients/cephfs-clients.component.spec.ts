import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, PermissionHelper } from '../../../../testing/unit-test-helper';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsClientsComponent } from './cephfs-clients.component';

describe('CephfsClientsComponent', () => {
  let component: CephfsClientsComponent;
  let fixture: ComponentFixture<CephfsClientsComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      ToastrModule.forRoot(),
      SharedModule,
      HttpClientTestingModule
    ],
    declarations: [CephfsClientsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsClientsComponent);
    component = fixture.componentInstance;
    component.clients = {
      status: ViewCacheStatus.ValueOk,
      data: [{}, {}, {}, {}]
    };
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
        actions: ['Evict'],
        primary: { multiple: 'Evict', executing: 'Evict', single: 'Evict', no: 'Evict' }
      },
      'create,update': {
        actions: ['Evict'],
        primary: { multiple: 'Evict', executing: 'Evict', single: 'Evict', no: 'Evict' }
      },
      'create,delete': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      },
      create: {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      },
      'update,delete': {
        actions: ['Evict'],
        primary: { multiple: 'Evict', executing: 'Evict', single: 'Evict', no: 'Evict' }
      },
      update: {
        actions: ['Evict'],
        primary: { multiple: 'Evict', executing: 'Evict', single: 'Evict', no: 'Evict' }
      },
      delete: {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });
});
