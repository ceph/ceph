import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SettingsService } from '~/app/shared/api/settings.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
import { UserTabsComponent } from '../user-tabs/user-tabs.component';
import { UserListComponent } from './user-list.component';

describe('UserListComponent', () => {
  let component: UserListComponent;
  let fixture: ComponentFixture<UserListComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      NgbNavModule,
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [UserListComponent, UserTabsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserListComponent);
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
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'create,update': {
        actions: ['Create', 'Edit'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'create,delete': {
        actions: ['Create', 'Delete'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      create: {
        actions: ['Create'],
        primary: {
          multiple: 'Create',
          executing: 'Create',
          single: 'Create',
          no: 'Create'
        }
      },
      'update,delete': {
        actions: ['Edit', 'Delete'],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      },
      update: {
        actions: ['Edit'],
        primary: {
          multiple: 'Edit',
          executing: 'Edit',
          single: 'Edit',
          no: 'Edit'
        }
      },
      delete: {
        actions: ['Delete'],
        primary: {
          multiple: 'Delete',
          executing: 'Delete',
          single: 'Delete',
          no: 'Delete'
        }
      },
      'no-permissions': {
        actions: [],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      }
    });
  });
  it('should calculate remaining days', () => {
    const day = 60 * 60 * 24 * 1000;
    let today = Date.now();
    expect(component.getRemainingDays(today + day * 2 + 1000)).toBe(2);
    today = Date.now();
    expect(component.getRemainingDays(today + day * 2 - 1000)).toBe(1);
    today = Date.now();
    expect(component.getRemainingDays(today + day + 1000)).toBe(1);
    today = Date.now();
    expect(component.getRemainingDays(today + 1)).toBe(0);
    today = Date.now();
    expect(component.getRemainingDays(today - (day + 1))).toBe(0);
    expect(component.getRemainingDays(null)).toBe(undefined);
    expect(component.getRemainingDays(undefined)).toBe(undefined);
  });
  describe('password expiry thresholds', () => {
    let settingsService: SettingsService;

    beforeEach(() => {
      settingsService = TestBed.inject(SettingsService);
    });

    it('should call getStandardSettings on init and set expiration thresholds', () => {
      spyOn(settingsService, 'getStandardSettings').and.returnValue(
        of({ user_pwd_expiration_warning_1: 10, user_pwd_expiration_warning_2: 5 })
      );
      fixture.detectChanges();
      expect(settingsService.getStandardSettings).toHaveBeenCalled();
      expect(component.expirationWarningAlert).toBe(10);
      expect(component.expirationDangerAlert).toBe(5);
    });

    it('should not call getValues on init', () => {
      spyOn(settingsService, 'getValues').and.callThrough();
      spyOn(settingsService, 'getStandardSettings').and.returnValue(of({}));
      fixture.detectChanges();
      expect(settingsService.getValues).not.toHaveBeenCalled();
    });
  });
});
