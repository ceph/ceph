import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ModalModule } from 'ngx-bootstrap/modal';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketDetailsComponent } from '../rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list.component';

describe('RgwBucketListComponent', () => {
  let component: RgwBucketListComponent;
  let fixture: ComponentFixture<RgwBucketListComponent>;

  configureTestBed({
    declarations: [RgwBucketListComponent, RgwBucketDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      RouterTestingModule,
      ModalModule.forRoot(),
      SharedModule,
      NgbNavModule,
      HttpClientTestingModule
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketListComponent);
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
        actions: ['Create', 'Edit', 'Delete'],
        primary: { multiple: 'Delete', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,update': {
        actions: ['Create', 'Edit'],
        primary: { multiple: 'Create', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Delete'],
        primary: { multiple: 'Delete', executing: 'Create', single: 'Create', no: 'Create' }
      },
      create: {
        actions: ['Create'],
        primary: { multiple: 'Create', executing: 'Create', single: 'Create', no: 'Create' }
      },
      'update,delete': {
        actions: ['Edit', 'Delete'],
        primary: { multiple: 'Delete', executing: 'Edit', single: 'Edit', no: 'Edit' }
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
