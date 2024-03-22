import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
import { RgwBucketDetailsComponent } from '../rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list.component';
import { ToastrModule } from 'ngx-toastr';

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
      SharedModule,
      NgbNavModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ]
  });

  beforeEach(() => {
    rgwBucketService = TestBed.inject(RgwBucketService);
    rgwBucketServiceListSpy = spyOn(rgwBucketService, 'list');
    rgwBucketServiceListSpy.and.returnValue(of([]));
    fixture = TestBed.createComponent(RgwBucketListComponent);
    component = fixture.componentInstance;
    spyOn(component, 'setTableRefreshTimeout').and.stub();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(rgwBucketServiceListSpy).toHaveBeenCalledTimes(1);
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

  it('should test if bucket data is transformed correctly', () => {
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
            'rgw.none': {
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
    component.getBucketList(null);
    expect(rgwBucketServiceListSpy).toHaveBeenCalledTimes(2);
    expect(component.buckets).toEqual([
      {
        bucket: 'bucket',
        owner: 'testid',
        usage: {
          'rgw.main': { size_actual: 4, num_objects: 2 },
          'rgw.none': { size_actual: 6, num_objects: 6 }
        },
        bucket_quota: {
          max_size: 20,
          max_objects: 10,
          enabled: true
        },
        bucket_size: 4,
        num_objects: 2,
        size_usage: 0.2,
        object_usage: 0.2
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
    component.getBucketList(null);
    expect(rgwBucketServiceListSpy).toHaveBeenCalledTimes(2);
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
    component.getBucketList(null);
    expect(rgwBucketServiceListSpy).toHaveBeenCalledTimes(2);
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(0);
  });
});
