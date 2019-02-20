import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalService } from 'ngx-bootstrap/modal';
import { of } from 'rxjs';

import { configureTestBed, FormHelper, i18nProviders } from '../../../../testing/unit-test-helper';
import { RoleService } from '../../../shared/api/role.service';
import { UserService } from '../../../shared/api/user.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { UserFormComponent } from './user-form.component';
import { UserFormModel } from './user-form.model';

describe('UserFormComponent', () => {
  let component: UserFormComponent;
  let form: CdFormGroup;
  let fixture: ComponentFixture<UserFormComponent>;
  let httpTesting: HttpTestingController;
  let userService: UserService;
  let modalService: BsModalService;
  let router: Router;
  let formHelper: FormHelper;

  const setUrl = (url) => Object.defineProperty(router, 'url', { value: url });

  @Component({ selector: 'cd-fake', template: '' })
  class FakeComponent {}

  const routes: Routes = [
    { path: 'login', component: FakeComponent },
    { path: 'users', component: FakeComponent }
  ];

  configureTestBed(
    {
      imports: [
        RouterTestingModule.withRoutes(routes),
        HttpClientTestingModule,
        ReactiveFormsModule,
        ComponentsModule,
        ToastModule.forRoot(),
        SharedModule
      ],
      declarations: [UserFormComponent, FakeComponent],
      providers: i18nProviders
    },
    true
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(UserFormComponent);
    component = fixture.componentInstance;
    form = component.userForm;
    httpTesting = TestBed.get(HttpTestingController);
    userService = TestBed.get(UserService);
    modalService = TestBed.get(BsModalService);
    router = TestBed.get(Router);
    spyOn(router, 'navigate');
    fixture.detectChanges();
    const notify = TestBed.get(NotificationService);
    spyOn(notify, 'show');
    formHelper = new FormHelper(form);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(form).toBeTruthy();
  });

  describe('create mode', () => {
    beforeEach(() => {
      setUrl('/user-management/users/add');
      component.ngOnInit();
    });

    it('should not disable fields', () => {
      ['username', 'name', 'password', 'confirmpassword', 'email', 'roles'].forEach((key) =>
        expect(form.get(key).disabled).toBeFalsy()
      );
    });

    it('should validate username required', () => {
      formHelper.expectErrorChange('username', '', 'required');
      formHelper.expectValidChange('username', 'user1');
    });

    it('should validate password match', () => {
      formHelper.setValue('password', 'aaa');
      formHelper.expectErrorChange('confirmpassword', 'bbb', 'match');
      formHelper.expectValidChange('confirmpassword', 'aaa');
    });

    it('should validate email', () => {
      formHelper.expectErrorChange('email', 'aaa', 'email');
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
        roles: ['administrator']
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
      roles: ['administrator']
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
      spyOn(TestBed.get(RoleService), 'list').and.callFake(() => of(roles));
      setUrl('/user-management/users/edit/user1');
      component.ngOnInit();
      const req = httpTesting.expectOne('api/role');
      expect(req.request.method).toBe('GET');
      req.flush(roles);
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

    it('should alert if user is removing needed role permission', () => {
      spyOn(TestBed.get(AuthStorageService), 'getUsername').and.callFake(() => user.username);
      let modalBodyTpl = null;
      spyOn(modalService, 'show').and.callFake((_content, config) => {
        modalBodyTpl = config.initialState.bodyTpl;
      });
      formHelper.setValue('roles', ['read-only']);
      component.submit();
      expect(modalBodyTpl).toEqual(component.removeSelfUserReadUpdatePermissionTpl);
    });

    it('should logout if current user roles have been changed', () => {
      spyOn(TestBed.get(AuthStorageService), 'getUsername').and.callFake(() => user.username);
      formHelper.setValue('roles', ['user-manager']);
      component.submit();
      const userReq = httpTesting.expectOne(`api/user/${user.username}`);
      expect(userReq.request.method).toBe('PUT');
      userReq.flush({});
      const authReq = httpTesting.expectOne('api/auth/logout');
      expect(authReq.request.method).toBe('POST');
    });

    it('should submit', () => {
      spyOn(TestBed.get(AuthStorageService), 'getUsername').and.callFake(() => user.username);
      component.submit();
      const userReq = httpTesting.expectOne(`api/user/${user.username}`);
      expect(userReq.request.method).toBe('PUT');
      expect(userReq.request.body).toEqual({
        username: 'user1',
        password: '',
        name: 'User 1',
        email: 'user1@email.com',
        roles: ['administrator']
      });
      userReq.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['/user-management/users']);
    });
  });
});
