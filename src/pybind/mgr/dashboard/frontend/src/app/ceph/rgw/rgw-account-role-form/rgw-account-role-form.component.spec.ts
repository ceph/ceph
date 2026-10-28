import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ReactiveFormsModule } from '@angular/forms';
import { ButtonModule, InputModule, ModalModule } from 'carbon-components-angular';
import { of, throwError } from 'rxjs';

import { RgwAccountRoleFormComponent } from './rgw-account-role-form.component';
import { SharedModule } from '~/app/shared/shared.module';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

describe('RgwAccountRoleFormComponent', () => {
  let component: RgwAccountRoleFormComponent;
  let fixture: ComponentFixture<RgwAccountRoleFormComponent>;
  let rgwRoleService: RgwRoleService;
  let notificationService: NotificationService;

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
    rgwRoleService = TestBed.inject(RgwRoleService);
    notificationService = TestBed.inject(NotificationService);
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

  it('should mark duplicate policy names as invalid with uniqueName error', () => {
    component.addPermissionPolicy('PolicyA', '{"Version": "2012-10-17"}');
    component.addPermissionPolicy('PolicyA', '{"Version": "2012-10-17"}');

    expect(
      component.permissionPolicies.at(0).get('policy_name').hasError('uniqueName')
    ).toBeTruthy();
    expect(
      component.permissionPolicies.at(1).get('policy_name').hasError('uniqueName')
    ).toBeTruthy();

    component.permissionPolicies.at(1).get('policy_name').setValue('PolicyB');
    expect(
      component.permissionPolicies.at(0).get('policy_name').hasError('uniqueName')
    ).toBeFalsy();
    expect(
      component.permissionPolicies.at(1).get('policy_name').hasError('uniqueName')
    ).toBeFalsy();
  });

  it('should keep form invalid when an empty permission policy row is added', () => {
    component.addPermissionPolicy();
    component.form.patchValue({
      role_name: 'newRole',
      role_path: '/',
      role_assume_policy_doc: '{}'
    });

    expect(component.permissionPolicies.length).toBe(1);
    expect(component.form.invalid).toBeTruthy();
    expect(component.permissionPolicies.at(0).get('policy_name').hasError('required')).toBeTruthy();
    expect(component.permissionPolicies.at(0).get('policy_doc').hasError('required')).toBeTruthy();
  });

  it('should mark NgForm submitted when submitting an empty create form', () => {
    component.onSubmit();

    expect(component.formDir.submitted).toBeTruthy();
    expect(component.form.get('role_name').invalid).toBeTruthy();
    expect(component.form.get('role_name').hasError('required')).toBeTruthy();
    expect(component.form.get('role_path').invalid).toBeTruthy();
    expect(component.form.get('role_assume_policy_doc').invalid).toBeTruthy();
    expect(component.form.invalid).toBeTruthy();
  });

  it('should block create and mark empty policy fields when a blank policy row is added', () => {
    spyOn(rgwRoleService, 'create');
    component.addPermissionPolicy();
    component.form.patchValue({
      role_name: 'newRole',
      role_path: '/',
      role_assume_policy_doc: '{}'
    });

    component.onSubmit();

    expect(rgwRoleService.create).not.toHaveBeenCalled();
    expect(component.formDir.submitted).toBeTruthy();
    expect(component.permissionPolicies.at(0).get('policy_name').touched).toBeTruthy();
    expect(component.permissionPolicies.at(0).get('policy_name').hasError('required')).toBeTruthy();
    expect(component.permissionPolicies.at(0).get('policy_doc').hasError('required')).toBeTruthy();
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

  it('should report failed policy names and keep tearsheet open', () => {
    spyOn(rgwRoleService, 'create').and.returnValue(of(null));
    spyOn(rgwRoleService, 'attachPolicy').and.callFake((_role, policyName) => {
      if (policyName === 'BadPolicy') {
        return throwError(() => new Error('attach failed'));
      }
      return of(null);
    });
    spyOn(notificationService, 'show');
    spyOn(component, 'closeModal');

    component.addPermissionPolicy('GoodPolicy', '{"Version":"2012-10-17"}');
    component.addPermissionPolicy('BadPolicy', '{"Version":"2012-10-17"}');
    component.form.patchValue({
      role_name: 'newRole',
      role_path: '/',
      role_assume_policy_doc: '{}'
    });

    component.onSubmit();

    expect(notificationService.show).toHaveBeenCalledWith(
      NotificationType.warning,
      jasmine.any(String),
      jasmine.stringMatching(/BadPolicy/)
    );
    expect(component.closeModal).not.toHaveBeenCalled();
    expect(component.isSubmitLoading).toBeFalsy();
  });
});
