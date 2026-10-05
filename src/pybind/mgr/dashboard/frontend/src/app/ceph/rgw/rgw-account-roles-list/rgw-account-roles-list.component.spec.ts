import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BehaviorSubject, of } from 'rxjs';

import { RgwAccountRolesListComponent } from './rgw-account-roles-list.component';
import { RgwRoleService } from '~/app/shared/api/rgw-role.service';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { SharedModule } from '~/app/shared/shared.module';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwDaemon } from '../models/rgw-daemon';

describe('RgwAccountRolesListComponent', () => {
  let component: RgwAccountRolesListComponent;
  let fixture: ComponentFixture<RgwAccountRolesListComponent>;
  let rgwRoleService: RgwRoleService;
  let notificationService: NotificationService;
  let selectedDaemon$: BehaviorSubject<RgwDaemon>;
  let zonegroupGet: jest.Mock;
  let modalShow: jest.Mock;

  const masterDaemon = {
    id: 'rgw.master',
    zonegroup_name: 'zg1',
    zone_name: 'master'
  } as RgwDaemon;

  const secondaryDaemon = {
    id: 'rgw.secondary',
    zonegroup_name: 'zg1',
    zone_name: 'secondary'
  } as RgwDaemon;

  const zonegroup = {
    name: 'zg1',
    master_zone: 'z1',
    zones: [
      { id: 'z1', name: 'master' },
      { id: 'z2', name: 'secondary' }
    ]
  };

  beforeEach(async () => {
    selectedDaemon$ = new BehaviorSubject<RgwDaemon>(masterDaemon);
    zonegroupGet = jest.fn().mockReturnValue(of(zonegroup));
    modalShow = jest.fn();

    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
      declarations: [RgwAccountRolesListComponent],
      providers: [
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: () => ({ rgw: { create: true, update: true, delete: true } })
          }
        },
        {
          provide: RgwDaemonService,
          useValue: { selectedDaemon$: selectedDaemon$.asObservable() }
        },
        {
          provide: RgwZonegroupService,
          useValue: { get: zonegroupGet }
        },
        {
          provide: ModalCdsService,
          useValue: { show: modalShow }
        }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwAccountRolesListComponent);
    component = fixture.componentInstance;
    rgwRoleService = TestBed.inject(RgwRoleService);
    notificationService = TestBed.inject(NotificationService);
    jest.spyOn(notificationService, 'show');
    component.accountId = 'test-account';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load roles on init', () => {
    const roles = [{ RoleName: 'test-role' }];
    jest.spyOn(rgwRoleService, 'list').mockReturnValue(of(roles as any));
    component.loadRoles();
    expect(rgwRoleService.list).toHaveBeenCalledWith('test-account');
    component.data$.subscribe((res) => {
      expect(res).toEqual(roles);
    });
  });

  it('should delete a role and show notification', () => {
    jest.spyOn(rgwRoleService, 'delete').mockReturnValue(of(null));
    jest.spyOn(component, 'loadRoles');
    component.selection.selected = [{ RoleName: 'test-role' }];
    modalShow.mockImplementation((_componentClass, config) => {
      config.submitActionObservable().subscribe();
      return null;
    });

    component.deleteRole();
    expect(rgwRoleService.delete).toHaveBeenCalledWith('test-role', 'test-account');
    expect(notificationService.show).toHaveBeenCalled();
    expect(component.loadRoles).toHaveBeenCalled();
  });

  it('should enable role mutations on the master zone', () => {
    expect(component.isMasterZone).toBe(true);
    expect(component.getRoleActionDisable(false)).toBe(false);
    expect(component.getRoleActionDisable(true)).toBe(true);
    component.selection.selected = [{ RoleName: 'test-role' }];
    expect(component.getRoleActionDisable(true)).toBe(false);
  });

  it('should disable role mutations on a secondary zone', () => {
    selectedDaemon$.next(secondaryDaemon);
    fixture.detectChanges();

    expect(component.isMasterZone).toBe(false);
    expect(component.getRoleActionDisable(false)).toEqual(expect.stringContaining('master zone'));
    expect(component.getRoleActionDisable(true)).toEqual(expect.stringContaining('master zone'));
  });

  it('should not open role form on a secondary zone', () => {
    selectedDaemon$.next(secondaryDaemon);
    modalShow.mockClear();

    component.openRoleForm(false);
    expect(modalShow).not.toHaveBeenCalled();
  });
});
