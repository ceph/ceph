import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { ModalModule } from 'ngx-bootstrap/modal';
import { of } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../testing/unit-test-helper';
import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { TableActionsComponent } from '../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketDetailsComponent } from '../rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list.component';

describe('RgwBucketListComponent', () => {
  let component: RgwBucketListComponent;
  let fixture: ComponentFixture<RgwBucketListComponent>;
  let rgwBucketService: RgwBucketService;
  let rgwBucketServiceListSpy: jasmine.Spy;

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
    rgwBucketService = TestBed.get(RgwBucketService);
    rgwBucketServiceListSpy = spyOn(rgwBucketService, 'list');
    rgwBucketServiceListSpy.and.returnValue(of(null));
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

  it('should test if bucket data is tranformed correctly', () => {
    rgwBucketServiceListSpy.and.returnValue(
      of([
        {
          bucket: 'bucket',
          owner: 'testid',
          usage: {
            'rgw.main': {
              size_actual: 4,
              num_objects: 2
            },
            'rgw.another': {
              size_actual: 6,
              num_objects: 6
            }
          },
          bucket_quota: {
            max_size: 20,
            max_objects: 10,
            enabled: true
          }
        }
      ])
    );
    fixture.detectChanges();
    expect(component.buckets).toEqual([
      {
        bucket: 'bucket',
        owner: 'testid',
        usage: {
          'rgw.main': { size_actual: 4, num_objects: 2 },
          'rgw.another': { size_actual: 6, num_objects: 6 }
        },
        bucket_quota: {
          max_size: 20,
          max_objects: 10,
          enabled: true
        },
        bucket_size: 10,
        num_objects: 8,
        size_usage: 0.5,
        object_usage: 0.8
      }
    ]);
  });
  it('should usage bars only if quota enabled', () => {
    rgwBucketServiceListSpy.and.returnValue(
      of([
        {
          bucket: 'bucket',
          owner: 'testid',
          bucket_quota: {
            max_size: 1024,
            max_objects: 10,
            enabled: true
          }
        }
      ])
    );
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(2);
  });
  it('should not show any usage bars if quota disabled', () => {
    rgwBucketServiceListSpy.and.returnValue(
      of([
        {
          bucket: 'bucket',
          owner: 'testid',
          bucket_quota: {
            max_size: 1024,
            max_objects: 10,
            enabled: false
          }
        }
      ])
    );
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(0);
  });
});
