import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ReactiveFormsModule } from '@angular/forms';
import {
  ButtonModule,
  InputModule,
  ModalModule,
  RadioModule,
  SelectModule
} from 'carbon-components-angular';

import { RgwAccountRoleFormComponent } from './rgw-account-role-form.component';
import { SharedModule } from '~/app/shared/shared.module';

describe('RgwAccountRoleFormComponent', () => {
  let component: RgwAccountRoleFormComponent;
  let fixture: ComponentFixture<RgwAccountRoleFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        SharedModule,
        ReactiveFormsModule,
        InputModule,
        ModalModule,
        ButtonModule
      ],
      declarations: [RgwAccountRoleFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwAccountRoleFormComponent);
    component = fixture.componentInstance;
    component.accountId = 'test-account';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create form correctly on init', () => {
    expect(component.form).toBeDefined();
    expect(component.form.contains('role_name')).toBeTruthy();
    expect(component.form.contains('role_path')).toBeTruthy();
    expect(component.form.contains('role_assume_policy_doc')).toBeTruthy();
    expect(component.form.contains('permission_policies')).toBeTruthy();
    expect(component.permissionPolicies.length).toBe(0);
  });

  it('should allow adding and removing permission policies', () => {
    expect(component.permissionPolicies.length).toBe(0);
    component.addPermissionPolicy('TestPolicy', '{"Version": "2012-10-17"}');
    expect(component.permissionPolicies.length).toBe(1);
    expect(component.permissionPolicies.at(0).get('policy_name').value).toBe('TestPolicy');

    component.removePermissionPolicy(0);
    expect(component.permissionPolicies.length).toBe(0);
  });

  it('should allow adding policy fields again after remove', () => {
    component.addPermissionPolicy();
    expect(component.permissionPolicies.length).toBe(1);

    component.removePermissionPolicy(0);
    expect(component.permissionPolicies.length).toBe(0);

    component.addPermissionPolicy();
    expect(component.permissionPolicies.length).toBe(1);
    expect(component.permissionPolicies.at(0).get('policy_name')).toBeDefined();
  });

  it('should ignore empty permission policy rows on submit', () => {
    component.addPermissionPolicy('', '', 'custom');
    component.form.patchValue({
      role_name: 'newRole',
      role_path: '/',
      role_assume_policy_doc: '{}'
    });

    component['removeEmptyPermissionPolicies']();

    expect(component.permissionPolicies.length).toBe(0);
    expect(component.form.valid).toBeTruthy();
  });

  it('should show validation errors when submitting an empty create form', () => {
    component.onSubmit();

    expect(component.formSubmitted).toBeTruthy();
    expect(component.isFieldInvalid('role_name')).toBeTruthy();
    expect(component.showFieldError('role_name', 'required')).toBeTruthy();
    expect(component.isFieldInvalid('role_path')).toBeTruthy();
    expect(component.isFieldInvalid('role_assume_policy_doc')).toBeTruthy();
    expect(component.form.invalid).toBeTruthy();
  });

  it('should not show policy field errors on a newly added empty row after submit', () => {
    component.onSubmit();
    expect(component.formSubmitted).toBeTruthy();

    component.addPermissionPolicy();
    const policy = component.permissionPolicies.at(0);

    expect(component.isPolicyFieldInvalid(policy, 'policy_name')).toBeFalsy();
    expect(component.isPolicyFieldInvalid(policy, 'policy_doc')).toBeFalsy();
    expect(component.showPolicyFieldError(policy, 'policy_name', 'required')).toBeFalsy();
  });

  it('should patch value in edit mode', () => {
    component.isEdit = true;
    component.roleName = 'test-role';
    component.role = {
      RoleName: 'test-role',
      Path: '/path',
      MaxSessionDuration: 3 * 3600
    } as any;

    component.ngOnInit();

    expect(component.form.get('role_name').value).toBe('test-role');
    expect(component.form.get('max_session_duration').value).toBe(3);
  });
});
