import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwAccountRoleDetailsComponent } from './rgw-account-role-details.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { SharedModule } from '~/app/shared/shared.module';

describe('RgwAccountRoleDetailsComponent', () => {
  let component: RgwAccountRoleDetailsComponent;
  let fixture: ComponentFixture<RgwAccountRoleDetailsComponent>;
  let rgwRoleService: RgwRoleService;

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
    declarations: [RgwAccountRoleDetailsComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwAccountRoleDetailsComponent);
    component = fixture.componentInstance;
    rgwRoleService = TestBed.inject(RgwRoleService);
    component.accountId = 'test-account';
    component.selection = { RoleName: 'test-role' } as any;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load policies', () => {
    spyOn(rgwRoleService, 'listPolicies').and.returnValue(of(['test-policy']));
    component.loadPolicies();
    component.policies$.subscribe((policies) => {
      expect(policies).toEqual([{ name: 'test-policy' }]);
    });
  });

  it('should emit policySelected when a policy is clicked', () => {
    spyOn(component.policySelected, 'emit');
    component.onPolicyClick('test-policy');
    expect(component.policySelected.emit).toHaveBeenCalledWith({
      roleName: 'test-role',
      policyName: 'test-policy'
    });
  });
});
