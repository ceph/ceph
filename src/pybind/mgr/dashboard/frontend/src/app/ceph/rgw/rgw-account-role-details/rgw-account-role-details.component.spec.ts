import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwAccountRoleDetailsComponent } from './rgw-account-role-details.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { SharedModule } from '~/app/shared/shared.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

describe('RgwAccountRoleDetailsComponent', () => {
  let component: RgwAccountRoleDetailsComponent;
  let fixture: ComponentFixture<RgwAccountRoleDetailsComponent>;
  let rgwRoleService: RgwRoleService;
  let notificationService: NotificationService;

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
    declarations: [RgwAccountRoleDetailsComponent],
    providers: [
      {
        provide: AuthStorageService,
        useValue: {
          getPermissions: () => ({ rgw: { create: true, update: true, delete: true } })
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwAccountRoleDetailsComponent);
    component = fixture.componentInstance;
    rgwRoleService = TestBed.inject(RgwRoleService);
    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show');
    component.accountId = 'test-account';
    component.selection = { RoleName: 'test-role' } as any;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load policies on init', () => {
    const policies = ['policy1', 'policy2'];
    spyOn(rgwRoleService, 'listPolicies').and.returnValue(of(policies));
    component.loadPolicies();
    expect(rgwRoleService.listPolicies).toHaveBeenCalledWith('test-role', 'test-account');
    component.policies$.subscribe((res) => {
      expect(res).toEqual([{ name: 'policy1' }, { name: 'policy2' }]);
    });
  });

  it('should delete a policy and show notification', () => {
    spyOn(rgwRoleService, 'deletePolicy').and.returnValue(of(null));
    spyOn(component, 'loadPolicies');
    component.policySelection.selected = [{ name: 'test-policy' }];
    spyOn(TestBed.inject(ModalCdsService), 'show').and.callFake((_componentClass, config) => {
      config.submitActionObservable().subscribe();
      return null;
    });

    component.deletePolicy();
    expect(rgwRoleService.deletePolicy).toHaveBeenCalledWith(
      'test-role',
      'test-policy',
      'test-account'
    );
    expect(notificationService.show).toHaveBeenCalled();
    expect(component.loadPolicies).toHaveBeenCalled();
  });

  it('should open edit policy modal', () => {
    const modalService = TestBed.inject(ModalCdsService);
    spyOn(modalService, 'show');
    component.policySelection.selected = [{ name: 'test-policy' }];
    component.openEditPolicyModal();
    expect(modalService.show).toHaveBeenCalled();
  });
});
