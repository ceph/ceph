import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { of } from 'rxjs';

import { RgwAccountRolesListComponent } from './rgw-account-roles-list.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { SharedModule } from '~/app/shared/shared.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

describe('RgwAccountRolesListComponent', () => {
  let component: RgwAccountRolesListComponent;
  let fixture: ComponentFixture<RgwAccountRolesListComponent>;
  let rgwRoleService: RgwRoleService;
  let notificationService: NotificationService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
      declarations: [RgwAccountRolesListComponent],
      providers: [
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: () => ({ rgw: { create: true, update: true, delete: true } })
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwAccountRolesListComponent);
    component = fixture.componentInstance;
    rgwRoleService = TestBed.inject(RgwRoleService);
    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show');
    component.accountId = 'test-account';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load roles on init', () => {
    const roles = [{ RoleName: 'test-role' }];
    spyOn(rgwRoleService, 'list').and.returnValue(of(roles));
    component.loadRoles();
    expect(rgwRoleService.list).toHaveBeenCalledWith('test-account');
    component.data$.subscribe((res) => {
      expect(res).toEqual(roles);
    });
  });

  it('should delete a role and show notification', () => {
    spyOn(rgwRoleService, 'delete').and.returnValue(of(null));
    spyOn(component, 'loadRoles');
    component.selection.selected = [{ RoleName: 'test-role' }];
    spyOn(TestBed.inject(ModalCdsService), 'show').and.callFake((_componentClass, config) => {
      config.submitActionObservable().subscribe();
      return null;
    });

    component.deleteRole();
    expect(rgwRoleService.delete).toHaveBeenCalledWith('test-role', 'test-account');
    expect(notificationService.show).toHaveBeenCalled();
    expect(component.loadRoles).toHaveBeenCalled();
  });

  it('should open policy side panel for a specific policy', fakeAsync(() => {
    component.openPolicyPanel('test-role', 'policy-2');
    tick();
    expect(component.policyPanelOpen).toBe(true);
    expect(component.selectedPolicyName).toBe('policy-2');
  }));
});
