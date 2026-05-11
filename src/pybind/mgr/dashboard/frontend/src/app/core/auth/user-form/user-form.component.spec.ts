import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';

import { of } from 'rxjs';

import { RoleService } from '~/app/shared/api/role.service';
import { SettingsService } from '~/app/shared/api/settings.service';
import { UserService } from '~/app/shared/api/user.service';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { PasswordPolicyService } from '~/app/shared/services/password-policy.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { UserFormComponent } from './user-form.component';
import { UserFormModel } from './user-form.model';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

describe('UserFormComponent', () => {
  let component: UserFormComponent;
  let form: CdFormGroup;
  let fixture: ComponentFixture<UserFormComponent>;
  let httpTesting: HttpTestingController;
  let userService: UserService;
  let modalService: ModalCdsService;
  let router: Router;
  let routerUrlSpy: jasmine.Spy<() => string>;
  let formHelper: FormHelper;

  @Component({ selector: 'cd-fake', template: '', standalone: false })
  class FakeComponent {}

  const routes: Routes = [
    { path: 'login', component: FakeComponent },
    { path: 'users', component: FakeComponent }
  ];

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      RouterTestingModule.withRoutes(routes),
      HttpClientTestingModule,
      ReactiveFormsModule,
      ComponentsModule,
      SharedModule,
      NgbPopoverModule
    ],
    declarations: [UserFormComponent, FakeComponent],
    providers: [{ provide: NotificationService, useValue: { show: () => undefined } }]
  });

  beforeEach(() => {
    spyOn(TestBed.inject(PasswordPolicyService), 'getHelpText').and.callFake(() => of(''));
    fixture = TestBed.createComponent(UserFormComponent);
    component = fixture.componentInstance;
    form = component.userForm;
    httpTesting = TestBed.inject(HttpTestingController);
    userService = TestBed.inject(UserService);
    spyOn(userService, 'validatePassword').and.returnValue(
      of({ valid: true, credits: 10, valuation: 'strong' })
    );
    modalService = TestBed.inject(ModalCdsService);
    router = TestBed.inject(Router);
    spyOn(router, 'navigate');
    routerUrlSpy = spyOnProperty(router, 'url', 'get').and.returnValue('/');
    spyOn(TestBed.inject(RoleService), 'list').and.callFake(() => of([]));
    spyOn(TestBed.inject(SettingsService), 'getStandardSettings').and.callFake(() =>
      of({
        user_pwd_expiration_warning_1: 10,
        user_pwd_expiration_warning_2: 5,
        user_pwd_expiration_span: 90
      })
    );
    fixture.detectChanges();
    const notify = TestBed.inject(NotificationService);
    spyOn(notify, 'show');
    formHelper = new FormHelper(form);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(form).toBeTruthy();
  });

  describe('create mode', () => {
    beforeEach(() => {
      routerUrlSpy.and.returnValue('/user-management/users/add');
      component.ngOnInit();
    });

    it('should set submit label to Create User', () => {
      expect(component.submitAction).toBe('Create User');
    });

    it('should not disable fields', () => {
      [
        'username',
        'name',
        'password',
        'confirmpassword',
        'email',
        'roles',
        'pwdExpirationDate'
      ].forEach((key) => expect(form.get(key).disabled).toBeFalsy());
    });

    it('should validate username required', () => {
      formHelper.expectErrorChange('username', '', 'required');
      formHelper.expectValidChange('username', 'user1');
    });

    it('should reject invalid usernames', () => {
      formHelper.expectErrorChange('username', '..', 'pattern');
      formHelper.expectErrorChange('username', '??', 'pattern');
      formHelper.expectErrorChange('username', 'user/name', 'pattern');
    });

    it('should validate password match', () => {
      formHelper.setValue('password', 'aaa');
      formHelper.expectErrorChange('confirmpassword', 'bbb', 'match');
      formHelper.expectValidChange('confirmpassword', 'aaa');
    });

    it('should validate email', () => {
      formHelper.expectErrorChange('email', 'aaa', 'email');
    });

    it('should validate password required in create mode', () => {
      formHelper.expectErrorChange('password', '', 'required');
      formHelper.expectValidChange('password', 'pass123');
    });

    it('should validate confirmpassword required in create mode', () => {
      formHelper.setValue('password', 'pass123');
      formHelper.expectErrorChange('confirmpassword', '', 'required');
      formHelper.expectValidChange('confirmpassword', 'pass123');
    });

    it('should allow empty roles in create mode', () => {
      formHelper.setValue('roles', []);
      expect(form.get('roles').valid).toBeTruthy();
    });

    it('should not require password by default', () => {
      formHelper.setValue('password', '');
      formHelper.setValue('confirmpassword', '');
      expect(form.get('password').valid).toBeTruthy();
      expect(form.get('confirmpassword').valid).toBeTruthy();
    });

    it('should set mode', () => {
      expect(component.mode).toBeUndefined();
    });

    it('should submit', () => {
      const user: UserFormModel = {
        username: 'user0',
        password: 'pass0',
        name: 'User 0',
        email: 'user0@email.com',
        roles: ['administrator'],
        enabled: true,
        pwdExpirationDate: undefined,
        pwdUpdateRequired: true
      };
      formHelper.setMultipleValues(user);
      formHelper.setValue('confirmpassword', user.password);
      component.submit();
      const userReq = httpTesting.expectOne('api/user');
      expect(userReq.request.method).toBe('POST');
      expect(userReq.request.body).toEqual(user);
      userReq.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['/user-management/users']);
    });
  });

  describe('edit mode', () => {
    const user: UserFormModel = {
      username: 'user1',
      password: undefined,
      name: 'User 1',
      email: 'user1@email.com',
      roles: ['administrator'],
      enabled: true,
      pwdExpirationDate: undefined,
      pwdUpdateRequired: true
    };
    const roles = [
      {
        name: 'administrator',
        description: 'Administrator',
        scopes_permissions: {
          user: ['create', 'delete', 'read', 'update']
        }
      },
      {
        name: 'read-only',
        description: 'Read-Only',
        scopes_permissions: {
          user: ['read']
        }
      },
      {
        name: 'user-manager',
        description: 'User Manager',
        scopes_permissions: {
          user: ['create', 'delete', 'read', 'update']
        }
      }
    ];

    beforeEach(() => {
      spyOn(userService, 'get').and.callFake(() => of(user));
      spyOn(TestBed.inject(AuthStorageService), 'getUsername').and.returnValue(user.username);
      (TestBed.inject(RoleService).list as jasmine.Spy).and.returnValue(of(roles));
      routerUrlSpy.and.returnValue('/user-management/users/edit/user1');
      component.ngOnInit();
      fixture.detectChanges();
    });

    afterEach(() => {
      httpTesting.verify();
    });

    it('should disable fields if editing', () => {
      expect(form.get('username').disabled).toBeTruthy();
      ['name', 'password', 'confirmpassword', 'email', 'roles'].forEach((key) =>
        expect(form.get(key).disabled).toBeFalsy()
      );
    });

    it('should set control values', () => {
      ['username', 'name', 'email', 'roles'].forEach((key) =>
        expect(form.getValue(key)).toBe(user[key])
      );
      ['password', 'confirmpassword'].forEach((key) => expect(form.getValue(key)).toBeFalsy());
    });

    it('should set mode', () => {
      expect(component.mode).toBe('editing');
    });

    it('should not validate password required in edit mode', () => {
      form.get('password').setValue('');
      expect(form.get('password').valid).toBeTruthy();
      form.get('confirmpassword').setValue('');
      expect(form.get('confirmpassword').valid).toBeTruthy();
    });

    it('should keep roles list enabled for current user', () => {
      const administratorRole = component.allRoles.find((role) => role.name === 'administrator');
      expect(administratorRole.enabled).toBeTruthy();
    });

    it('should allow clearing roles', () => {
      formHelper.setValue('roles', []);
      expect(form.getValue('roles')).toEqual([]);
    });

    it('should set submit label to Save changes', () => {
      expect(component.submitAction).toBe('Save changes');
    });

    it('should alert if user is removing needed role permission', () => {
      let modalBodyTpl = null;
      spyOn(modalService, 'show').and.callFake((_content, initialState) => {
        modalBodyTpl = initialState.bodyTpl;
      });
      formHelper.setValue('roles', ['read-only']);
      component.submit();
      expect(modalBodyTpl).toEqual(component.removeSelfUserReadUpdatePermissionTpl);
    });

    it('should logout if current user roles have been changed', () => {
      formHelper.setValue('roles', ['user-manager']);
      component.submit();
      const userReq = httpTesting.expectOne(`api/user/${user.username}`);
      expect(userReq.request.method).toBe('PUT');
      userReq.flush({});
      const authReq = httpTesting.expectOne('api/auth/logout');
      expect(authReq.request.method).toBe('POST');
    });

    it('should submit', () => {
      component.submit();
      const userReq = httpTesting.expectOne(`api/user/${user.username}`);
      expect(userReq.request.method).toBe('PUT');
      expect(userReq.request.body).toEqual({
        username: 'user1',
        password: '',
        pwdUpdateRequired: true,
        name: 'User 1',
        email: 'user1@email.com',
        roles: ['administrator'],
        enabled: true
      });
      userReq.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['/user-management/users']);
    });
  });
});
