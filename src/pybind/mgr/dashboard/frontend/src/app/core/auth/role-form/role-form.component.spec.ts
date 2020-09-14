import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { configureTestBed, FormHelper } from '../../../../testing/unit-test-helper';
import { RoleService } from '../../../shared/api/role.service';
import { ScopeService } from '../../../shared/api/scope.service';
import { LoadingPanelComponent } from '../../../shared/components/loading-panel/loading-panel.component';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RoleFormComponent } from './role-form.component';
import { RoleFormModel } from './role-form.model';

describe('RoleFormComponent', () => {
  let component: RoleFormComponent;
  let form: CdFormGroup;
  let fixture: ComponentFixture<RoleFormComponent>;
  let httpTesting: HttpTestingController;
  let roleService: RoleService;
  let router: Router;
  const setUrl = (url: string) => Object.defineProperty(router, 'url', { value: url });

  @Component({ selector: 'cd-fake', template: '' })
  class FakeComponent {}

  const routes: Routes = [{ path: 'roles', component: FakeComponent }];

  configureTestBed(
    {
      imports: [
        RouterTestingModule.withRoutes(routes),
        HttpClientTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        SharedModule
      ],
      declarations: [RoleFormComponent, FakeComponent]
    },
    [LoadingPanelComponent]
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(RoleFormComponent);
    component = fixture.componentInstance;
    form = component.roleForm;
    httpTesting = TestBed.inject(HttpTestingController);
    roleService = TestBed.inject(RoleService);
    router = TestBed.inject(Router);
    spyOn(router, 'navigate');
    fixture.detectChanges();
    const notify = TestBed.inject(NotificationService);
    spyOn(notify, 'show');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(form).toBeTruthy();
  });

  describe('create mode', () => {
    let formHelper: FormHelper;

    beforeEach(() => {
      setUrl('/user-management/roles/add');
      component.ngOnInit();
      formHelper = new FormHelper(form);
    });

    it('should not disable fields', () => {
      ['name', 'description', 'scopes_permissions'].forEach((key) =>
        expect(form.get(key).disabled).toBeFalsy()
      );
    });

    it('should validate name required', () => {
      formHelper.expectErrorChange('name', '', 'required');
    });

    it('should set mode', () => {
      expect(component.mode).toBeUndefined();
    });

    it('should submit', () => {
      const role: RoleFormModel = {
        name: 'role1',
        description: 'Role 1',
        scopes_permissions: { osd: ['read'] }
      };
      formHelper.setMultipleValues(role);
      component.submit();
      const roleReq = httpTesting.expectOne('api/role');
      expect(roleReq.request.method).toBe('POST');
      expect(roleReq.request.body).toEqual(role);
      roleReq.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['/user-management/roles']);
    });

    it('should check all perms for a scope', () => {
      formHelper.setValue('scopes_permissions', { cephfs: ['read'] });
      component.onClickCellCheckbox('grafana', 'scope');
      const scopes_permissions = form.getValue('scopes_permissions');
      expect(Object.keys(scopes_permissions)).toContain('grafana');
      expect(scopes_permissions['grafana']).toEqual(['create', 'delete', 'read', 'update']);
    });

    it('should uncheck all perms for a scope', () => {
      formHelper.setValue('scopes_permissions', { cephfs: ['read', 'create', 'update', 'delete'] });
      component.onClickCellCheckbox('cephfs', 'scope');
      const scopes_permissions = form.getValue('scopes_permissions');
      expect(Object.keys(scopes_permissions)).not.toContain('cephfs');
    });

    it('should uncheck all scopes and perms', () => {
      component.scopes = ['cephfs', 'grafana'];
      formHelper.setValue('scopes_permissions', {
        cephfs: ['read', 'delete'],
        grafana: ['update']
      });
      component.onClickHeaderCheckbox('scope', ({
        target: { checked: false }
      } as unknown) as Event);
      const scopes_permissions = form.getValue('scopes_permissions');
      expect(scopes_permissions).toEqual({});
    });

    it('should check all scopes and perms', () => {
      component.scopes = ['cephfs', 'grafana'];
      formHelper.setValue('scopes_permissions', {
        cephfs: ['create', 'update'],
        grafana: ['delete']
      });
      component.onClickHeaderCheckbox('scope', ({ target: { checked: true } } as unknown) as Event);
      const scopes_permissions = form.getValue('scopes_permissions');
      const keys = Object.keys(scopes_permissions);
      expect(keys).toEqual(['cephfs', 'grafana']);
      keys.forEach((key) => {
        expect(scopes_permissions[key].sort()).toEqual(['create', 'delete', 'read', 'update']);
      });
    });

    it('should check if column is checked', () => {
      component.scopes_permissions = [
        { scope: 'a', read: true, create: true, update: true, delete: true },
        { scope: 'b', read: false, create: true, update: false, delete: true }
      ];
      expect(component.isRowChecked('a')).toBeTruthy();
      expect(component.isRowChecked('b')).toBeFalsy();
      expect(component.isRowChecked('c')).toBeFalsy();
    });

    it('should check if header is checked', () => {
      component.scopes_permissions = [
        { scope: 'a', read: true, create: true, update: false, delete: true },
        { scope: 'b', read: false, create: true, update: false, delete: true }
      ];
      expect(component.isHeaderChecked('read')).toBeFalsy();
      expect(component.isHeaderChecked('create')).toBeTruthy();
      expect(component.isHeaderChecked('update')).toBeFalsy();
    });
  });

  describe('edit mode', () => {
    const role: RoleFormModel = {
      name: 'role1',
      description: 'Role 1',
      scopes_permissions: { osd: ['read', 'create'] }
    };
    const scopes = ['osd', 'user'];
    beforeEach(() => {
      spyOn(roleService, 'get').and.callFake(() => of(role));
      spyOn(TestBed.inject(ScopeService), 'list').and.callFake(() => of(scopes));
      setUrl('/user-management/roles/edit/role1');
      component.ngOnInit();
      const reqScopes = httpTesting.expectOne('ui-api/scope');
      expect(reqScopes.request.method).toBe('GET');
    });

    afterEach(() => {
      httpTesting.verify();
    });

    it('should disable fields if editing', () => {
      expect(form.get('name').disabled).toBeTruthy();
      ['description', 'scopes_permissions'].forEach((key) =>
        expect(form.get(key).disabled).toBeFalsy()
      );
    });

    it('should set control values', () => {
      ['name', 'description', 'scopes_permissions'].forEach((key) =>
        expect(form.getValue(key)).toBe(role[key])
      );
    });

    it('should set mode', () => {
      expect(component.mode).toBe('editing');
    });

    it('should submit', () => {
      component.onClickCellCheckbox('osd', 'update');
      component.onClickCellCheckbox('osd', 'create');
      component.onClickCellCheckbox('user', 'read');
      component.submit();
      const roleReq = httpTesting.expectOne(`api/role/${role.name}`);
      expect(roleReq.request.method).toBe('PUT');
      expect(roleReq.request.body).toEqual({
        name: 'role1',
        description: 'Role 1',
        scopes_permissions: { osd: ['read', 'update'], user: ['read'] }
      });
      roleReq.flush({});
      expect(router.navigate).toHaveBeenCalledWith(['/user-management/roles']);
    });
  });
});
