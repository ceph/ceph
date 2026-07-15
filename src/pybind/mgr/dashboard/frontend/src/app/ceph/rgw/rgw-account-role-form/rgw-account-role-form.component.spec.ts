import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ReactiveFormsModule } from '@angular/forms';

import { RgwAccountRoleFormComponent } from './rgw-account-role-form.component';
import { SharedModule } from '~/app/shared/shared.module';

describe('RgwAccountRoleFormComponent', () => {
  let component: RgwAccountRoleFormComponent;
  let fixture: ComponentFixture<RgwAccountRoleFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule, SharedModule, ReactiveFormsModule],
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
