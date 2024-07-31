import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
import { RgwUserListComponent } from './rgw-user-list.component';

describe('RgwUserListComponent', () => {
  let component: RgwUserListComponent;
  let fixture: ComponentFixture<RgwUserListComponent>;
  let rgwUserService: RgwUserService;
  let rgwUserServiceListSpy: jasmine.Spy;

  configureTestBed({
    declarations: [RgwUserListComponent],
    imports: [BrowserAnimationsModule, RouterTestingModule, HttpClientTestingModule, SharedModule],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    rgwUserService = TestBed.inject(RgwUserService);
    rgwUserServiceListSpy = spyOn(rgwUserService, 'list');
    rgwUserServiceListSpy.and.returnValue(of([]));
    fixture = TestBed.createComponent(RgwUserListComponent);
    component = fixture.componentInstance;
    spyOn(component, 'setTableRefreshTimeout').and.stub();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(rgwUserServiceListSpy).toHaveBeenCalledTimes(1);
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

  it('should test if rgw-user data is transformed correctly', () => {
    rgwUserServiceListSpy.and.returnValue(
      of([
        {
          user_id: 'testid',
          stats: {
            size_actual: 6,
            num_objects: 6
          },
          user_quota: {
            max_size: 20,
            max_objects: 10,
            enabled: true
          }
        }
      ])
    );
    component.getUserList(null);
    expect(rgwUserServiceListSpy).toHaveBeenCalledTimes(2);
    expect(component.users).toEqual([
      {
        user_id: 'testid',
        stats: {
          size_actual: 6,
          num_objects: 6
        },
        user_quota: {
          max_size: 20,
          max_objects: 10,
          enabled: true
        }
      }
    ]);
  });

  it('should usage bars only if quota enabled', () => {
    rgwUserServiceListSpy.and.returnValue(
      of([
        {
          user_id: 'testid',
          stats: {
            size_actual: 6,
            num_objects: 6
          },
          user_quota: {
            max_size: 1024,
            max_objects: 10,
            enabled: true
          }
        }
      ])
    );
    component.getUserList(null);
    expect(rgwUserServiceListSpy).toHaveBeenCalledTimes(2);
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(2);
  });

  it('should not show any usage bars if quota disabled', () => {
    rgwUserServiceListSpy.and.returnValue(
      of([
        {
          user_id: 'testid',
          stats: {
            size_actual: 6,
            num_objects: 6
          },
          user_quota: {
            max_size: 1024,
            max_objects: 10,
            enabled: false
          }
        }
      ])
    );
    component.getUserList(null);
    expect(rgwUserServiceListSpy).toHaveBeenCalledTimes(2);
    fixture.detectChanges();
    const usageBars = fixture.debugElement.nativeElement.querySelectorAll('cd-usage-bar');
    expect(usageBars.length).toBe(0);
  });
});
