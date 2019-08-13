import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';

import { ToastrModule } from 'ngx-toastr';
import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../shared/shared.module';
import { CephfsClientsComponent } from './cephfs-clients.component';

describe('CephfsClientsComponent', () => {
  let component: CephfsClientsComponent;
  let fixture: ComponentFixture<CephfsClientsComponent>;

  configureTestBed({
    imports: [
      RouterTestingModule,
      ToastrModule.forRoot(),
      BsDropdownModule.forRoot(),
      SharedModule,
      HttpClientTestingModule
    ],
    declarations: [CephfsClientsComponent],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsClientsComponent);
    component = fixture.componentInstance;
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
