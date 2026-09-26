import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { RgwAccountRolePolicyFormComponent } from './rgw-account-role-policy-form.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { SharedModule } from '~/app/shared/shared.module';
import { NotificationService } from '~/app/shared/services/notification.service';

describe('RgwAccountRolePolicyFormComponent', () => {
  let component: RgwAccountRolePolicyFormComponent;
  let fixture: ComponentFixture<RgwAccountRolePolicyFormComponent>;
  let rgwRoleService: RgwRoleService;
  let notificationService: NotificationService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule, SharedModule, ReactiveFormsModule],
      declarations: [RgwAccountRolePolicyFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwAccountRolePolicyFormComponent);
    component = fixture.componentInstance;
    rgwRoleService = TestBed.inject(RgwRoleService);
    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show');
    component.accountId = 'test-account';
    component.roleName = 'test-role';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should attach policy when form is submitted', () => {
    spyOn(rgwRoleService, 'putPolicy').and.returnValue(of(null));
    spyOn(component, 'closeModal');

    component.form.patchValue({
      policy_name: 'test-policy',
      policy_doc: '{"Statement":[]}'
    });

    component.onSubmit();

    expect(rgwRoleService.putPolicy).toHaveBeenCalledWith(
      'test-role',
      'test-policy',
      '{"Statement":[]}',
      'test-account'
    );
    expect(notificationService.show).toHaveBeenCalled();
    expect(component.closeModal).toHaveBeenCalled();
  });

  it('should load policy and update when in edit mode', () => {
    component.isEdit = true;
    component.policyName = 'test-policy';
    spyOn(rgwRoleService, 'getPolicy').and.returnValue(of({ Statement: [] }));
    spyOn(rgwRoleService, 'putPolicy').and.returnValue(of(null));
    spyOn(component, 'closeModal');

    component.ngOnInit();

    expect(rgwRoleService.getPolicy).toHaveBeenCalledWith(
      'test-role',
      'test-policy',
      'test-account'
    );
    expect(component.form.getRawValue().policy_name).toBe('test-policy');

    component.onSubmit();

    expect(rgwRoleService.putPolicy).toHaveBeenCalled();
    expect(notificationService.show).toHaveBeenCalled();
    expect(component.closeModal).toHaveBeenCalled();
  });
});
